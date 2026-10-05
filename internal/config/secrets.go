package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
)

// SecretScheme prefixes a string value that names a secret instead of
// holding it: "secret://oci-s3-key". See ResolveSecret.
const SecretScheme = "secret://"

// Secret sources, in lookup order.
const (
	// SecretsDirEnv names a directory holding one file per secret (mounted
	// Kubernetes/ECS/Container Apps secrets, systemd credentials).
	SecretsDirEnv = "PORTAGE_SECRETS_DIR"
	// SecretEnvPrefix + upper-cased name (with - and . as _) is the
	// environment fallback.
	SecretEnvPrefix = "PORTAGE_SECRET_"
)

// secretNameRE keeps secret names usable as file names without path tricks
// and mappable to environment variables.
var secretNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// SecretEnvVar returns the environment variable consulted for secret name.
func SecretEnvVar(name string) string {
	return SecretEnvPrefix + strings.ToUpper(strings.NewReplacer("-", "_", ".", "_").Replace(name))
}

// ResolveSecret returns the value of secret name: the file
// $PORTAGE_SECRETS_DIR/<name> (one trailing newline trimmed) if it exists,
// else the environment variable PORTAGE_SECRET_<NAME>. Errors never contain
// the value.
func ResolveSecret(name string) (string, error) {
	if err := checkSecretName(name); err != nil {
		return "", err
	}
	if dir := os.Getenv(SecretsDirEnv); dir != "" {
		b, err := os.ReadFile(filepath.Join(dir, name))
		switch {
		case err == nil:
			s := strings.TrimSuffix(string(b), "\n")
			return strings.TrimSuffix(s, "\r"), nil
		case !errors.Is(err, fs.ErrNotExist):
			var pe *fs.PathError
			if errors.As(err, &pe) {
				err = pe.Err
			}
			return "", fmt.Errorf("read %s/%s: %w", SecretsDirEnv, name, err)
		}
	}
	if v, ok := os.LookupEnv(SecretEnvVar(name)); ok {
		return v, nil
	}
	where := "environment variable " + SecretEnvVar(name)
	if os.Getenv(SecretsDirEnv) != "" {
		where = "$" + SecretsDirEnv + "/" + name + " or " + where
	} else {
		where += " (" + SecretsDirEnv + " is not set)"
	}
	return "", errors.New("not found in " + where)
}

func checkSecretName(name string) error {
	if !secretNameRE.MatchString(name) {
		return errors.New("secret name must be letters, digits, '.', '_' or '-', starting with a letter or digit")
	}
	return nil
}

// placeholderSecret validates a reference without resolving it. The
// placeholder is long enough to pass length checks (webhook_secret).
func placeholderSecret(name string) (string, error) {
	if err := checkSecretName(name); err != nil {
		return "", err
	}
	return "secret-placeholder-" + name, nil
}

// resolveSecrets replaces every string field of p (including string slice
// elements) of the form secret://<name> with the secret's value. Field paths
// in errors use the YAML names, prefixed with at.
func resolveSecrets(p *Pipeline, at string, resolve func(string) (string, error)) []error {
	var errs []error
	walkStrings(reflect.ValueOf(p).Elem(), at, func(path string, v reflect.Value) {
		ref := v.String()
		name, ok := strings.CutPrefix(ref, SecretScheme)
		if !ok {
			return
		}
		val, err := resolve(name)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %s: %w", path, ref, err))
			return
		}
		v.SetString(val)
	})
	return errs
}

// walkStrings calls fn for every settable string reachable from v through
// struct fields (named by their yaml tag), pointers and slices.
func walkStrings(v reflect.Value, path string, fn func(path string, v reflect.Value)) {
	switch v.Kind() {
	case reflect.String:
		fn(path, v)
	case reflect.Pointer:
		if !v.IsNil() {
			walkStrings(v.Elem(), path, fn)
		}
	case reflect.Slice:
		for i := range v.Len() {
			walkStrings(v.Index(i), fmt.Sprintf("%s[%d]", path, i), fn)
		}
	case reflect.Struct:
		t := v.Type()
		for i := range t.NumField() {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
			if !f.IsExported() || name == "" || name == "-" || name == "name" {
				continue
			}
			walkStrings(v.Field(i), joinPath(path, name), fn)
		}
	default:
	}
}
