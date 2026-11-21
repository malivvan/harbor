package functions

import "github.com/malivvan/storm/internal/constraints"

func Find_Go[T constraints.Float](x []T, a T) int {
	for i, v := range x {
		if v == a {
			return i
		}
	}
	return -1
}
