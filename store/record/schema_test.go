package record

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type schemaDoc struct {
	ID   string `db:"id"`
	Name string
	Kind string
}

func TestTypeSchemaRequiredEnumAndDefaults(t *testing.T) {
	cfg := Config{}.WithSchema(TypeSchema{
		Type:     &schemaDoc{},
		Required: []string{"Name"},
		Enums:    map[string][]string{"Kind": {"human", "agent"}},
		Defaults: map[string]any{"Kind": "human"},
	})
	store, _ := openTombstoneStore(t, cfg)
	ctx := context.Background()

	err := store.Save(ctx, &schemaDoc{ID: "a"})
	require.ErrorIs(t, err, ErrSchemaInvalid)

	require.NoError(t, store.Save(ctx, &schemaDoc{ID: "a", Name: "alice"}))
	var got schemaDoc
	require.NoError(t, store.One(ctx, "ID", "a", &got))
	require.Equal(t, "human", got.Kind)

	require.NoError(t, store.Save(ctx, &schemaDoc{ID: "b", Name: "bob", Kind: "agent"}))
	require.NoError(t, store.One(ctx, "ID", "b", &got))
	require.Equal(t, "agent", got.Kind)

	err = store.Save(ctx, &schemaDoc{ID: "c", Name: "carol", Kind: "robot"})
	require.ErrorIs(t, err, ErrSchemaInvalid)

	require.NoError(t, store.Validate(ctx, &schemaDoc{ID: "d", Name: "dave"}))
	require.ErrorIs(t, store.Validate(ctx, &schemaDoc{ID: "e"}), ErrSchemaInvalid)
}

func TestTypeSchemaDisabledIsPermissive(t *testing.T) {
	store, _ := openTombstoneStore(t, Config{})
	ctx := context.Background()
	require.NoError(t, store.Save(ctx, &schemaDoc{ID: "a"}))
	require.NoError(t, store.Validate(ctx, &schemaDoc{ID: "b"}))
}

func TestMigrateTypesAppliesStepsOnce(t *testing.T) {
	store, bdb := openTombstoneStore(t, Config{})
	ctx := context.Background()
	require.NoError(t, store.Save(ctx, &schemaDoc{ID: "a", Name: "alice"}))
	require.NoError(t, store.Save(ctx, &schemaDoc{ID: "b", Name: "bob"}))

	schema := TypeSchema{
		Type:    &schemaDoc{},
		Version: 1,
		Steps: []TypeStep{{
			From: 0,
			To:   1,
			Apply: func(ctx context.Context, tx Tx) error {
				var rows []schemaDoc
				err := tx.Select().Find(ctx, &rows)
				if err != nil && !errors.Is(err, ErrNotFound) {
					return err
				}
				for i := range rows {
					rows[i].Kind = "migrated"
					if err := tx.Save(ctx, &rows[i]); err != nil {
						return err
					}
				}
				return nil
			},
		}},
	}
	reopened, err := Open(ctx, bdb, Config{}.WithSchema(schema))
	require.NoError(t, err)
	require.NoError(t, reopened.MigrateTypes(ctx))

	var rows []schemaDoc
	require.NoError(t, reopened.All(ctx, &rows))
	require.Len(t, rows, 2)
	for _, row := range rows {
		require.Equal(t, "migrated", row.Kind)
	}

	// A second call is a no-op because the version is already current.
	require.NoError(t, reopened.MigrateTypes(ctx))
	require.NoError(t, reopened.All(ctx, &rows))
	require.Len(t, rows, 2)

	// A store ahead of the build is refused.
	older, err := Open(ctx, bdb, Config{}.WithSchema(TypeSchema{Type: &schemaDoc{}}))
	require.NoError(t, err)
	require.ErrorIs(t, older.MigrateTypes(ctx), ErrTypeSchemaTooNew)
}

func TestMigrateTypesRollsBack(t *testing.T) {
	store, bdb := openTombstoneStore(t, Config{})
	ctx := context.Background()
	require.NoError(t, store.Save(ctx, &schemaDoc{ID: "a", Name: "alice"}))

	schema := TypeSchema{
		Type:    &schemaDoc{},
		Version: 1,
		Steps: []TypeStep{{
			From: 0,
			To:   1,
			Apply: func(ctx context.Context, tx Tx) error {
				return errors.New("boom")
			},
		}},
	}
	reopened, err := Open(ctx, bdb, Config{}.WithSchema(schema))
	require.NoError(t, err)
	require.Error(t, reopened.MigrateTypes(ctx))

	var rows []schemaDoc
	require.NoError(t, reopened.All(ctx, &rows))
	require.Len(t, rows, 1)
	require.Empty(t, rows[0].Kind)
}

func TestTypeSchemaValidation(t *testing.T) {
	_, err := compileSchema(TypeSchema{})
	require.ErrorIs(t, err, ErrInvalidTypeSchema)
	_, err = compileSchema(TypeSchema{Type: &schemaDoc{}, Required: []string{"Missing"}})
	require.ErrorIs(t, err, ErrInvalidTypeSchema)
	_, err = compileSchema(TypeSchema{Type: &schemaDoc{}, Defaults: map[string]any{"Name": 42}})
	require.ErrorIs(t, err, ErrInvalidTypeSchema)
	_, err = compileSchema(TypeSchema{Type: &schemaDoc{}, Version: 1})
	require.ErrorIs(t, err, ErrInvalidTypeSchema)
}
