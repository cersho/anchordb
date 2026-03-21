package notifications

import (
	"context"
	"strings"
	"testing"

	"anchordb/internal/models"
)

func TestSendTestNotificationRejectsUnknownSMTPSecurity(t *testing.T) {
	dispatcher := &Dispatcher{}

	err := dispatcher.SendTestNotification(context.Background(), models.NotificationDestination{
		Type:         "smtp",
		SMTPHost:     "smtp.example.com",
		SMTPPort:     587,
		SMTPFrom:     "alerts@example.com",
		SMTPTo:       "ops@example.com",
		SMTPSecurity: "starttl",
	})
	if err == nil {
		t.Fatal("expected unsupported smtp_security error")
	}
	if !strings.Contains(err.Error(), "unsupported smtp_security") {
		t.Fatalf("expected unsupported smtp_security in error, got %v", err)
	}
}
