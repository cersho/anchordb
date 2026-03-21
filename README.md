# AnchorDB

AnchorDB is a lightweight Go service for automated database backups.
> [!WARNING]
> AnchorDB is still in active development and is not ready for production use.

<img width="1920" height="1032" alt="Screenshot 2026-03-20 173241" src="https://github.com/user-attachments/assets/774e1c0d-3dcb-4aa9-853e-369fdf49f377" />

## Features

- Supported databases: PostgreSQL, MySQL, Convex, and Cloudflare D1
- Automated backup scheduling
- API and web interface for connection and backup management
- Notifications: Discord webhooks and SMTP destinations with per-schedule success/failure routing, test send, and SMTP security modes (STARTTLS, SSL/TLS, none)
- Encryption and restore *(in progress)*
- Open source (OSS)

## License

This project is licensed under the Apache 2.0 License - see the [License](https://github.com/cersho/anchordb/edit/main/LICENSE) file for details.
