# Security Policy

Storm stores user data and, optionally, protects it with a password-derived
key. Security reports are taken seriously.

## Reporting a vulnerability

Please report suspected vulnerabilities privately to the maintainer
(malivvan) through GitHub's private security advisory feature on
<https://github.com/malivvan/storm/security/advisories/new>. Do not open a
public issue for an unfixed vulnerability.

Include the affected version or commit, a description, and a minimal
reproduction if possible. You can expect an acknowledgement within a few
days and a coordinated disclosure once a fix is available.

## Supported versions

The project has not published a stable release yet. Security fixes target the
`master` branch; tagged releases will be listed here when they exist.

## Scope

In scope: the encryption and key-derivation code, on-disk parsers, the
filesystem and time-series storage layers, and the S3 HTTP client. Out of
scope: vulnerabilities in dependencies (report them upstream) and issues that
require the attacker to already control the process or the database file
(except where they cause a crash on corrupt but authenticated input).
