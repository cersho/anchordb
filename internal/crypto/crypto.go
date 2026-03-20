package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
)

const prefix = "enc:v1:"

type Service struct {
	key []byte
}

func New(passphrase string) *Service {
	sum := sha256.Sum256([]byte(passphrase))
	return &Service{key: sum[:]}
}

func (s *Service) EncryptString(plain string) (string, error) {
	if plain == "" || strings.HasPrefix(plain, prefix) {
		return plain, nil
	}

	block, err := aes.NewCipher(s.key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	ciphertext := gcm.Seal(nil, nonce, []byte(plain), nil)
	payload := append(nonce, ciphertext...)
	return prefix + base64.StdEncoding.EncodeToString(payload), nil
}

func (s *Service) DecryptString(value string) (string, error) {
	if value == "" || !strings.HasPrefix(value, prefix) {
		return value, nil
	}

	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, prefix))
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(s.key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	if len(raw) < gcm.NonceSize() {
		return "", fmt.Errorf("invalid encrypted payload")
	}

	nonce := raw[:gcm.NonceSize()]
	ciphertext := raw[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}

	return string(plain), nil
}
