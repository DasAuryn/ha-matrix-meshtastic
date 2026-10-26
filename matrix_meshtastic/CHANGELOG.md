# 0.3.1

- Sichtbarer Importfortschritt und verständliches Ergebnis mit Schlüssel- und Nachrichtenzahlen.
- Schlüsselimport mit UTF-8-Passphrasen und großen Element-Exporten verbessert.
- Beendete Gerätebestätigungen verdecken die Geräteverwaltung nicht mehr.
- Bei blockiertem Versand werden die dafür zu bestätigenden Geräte hervorgehoben.

# 0.3.0

- Einrichtung und Verwaltung von Matrix, Meshtastic und Weiterleitung direkt in Ingress.
- Die Oberfläche startet auch ohne vorhandene Matrix-Anmeldedaten.
- Einstellungen lassen sich sicher speichern und ohne Neustart der Oberfläche anwenden.
- Passwortgeschützte Element-Schlüsselexporte können importiert werden, um ältere Nachrichten zu entschlüsseln.
- Räume erstellen, beitreten, umbenennen, Personen einladen und Räume verlassen.
- Sicherer Kontowechsel mit getrennten Ansichten und bestätigtem Abmelden.
- Verständliche Hinweise bei fehlenden Schlüsseln und nicht bestätigten Geräten sowie explizite Fingerabdruck-Bestätigung.

# 0.2.0

- Eigenständiger Matrix-Messenger über Home-Assistant-Ingress mit Räumen und Direktnachrichten.
- Matrix synchronisiert unabhängig von Meshtastic und speichert den Chatverlauf lokal.
- Meshtastic ist optional. Ein fehlender Schlüssel oder ein nicht verbundenes Funkgerät beendet den Messenger nicht.
- Nur der ausdrücklich konfigurierte Bridge-Raum wird auf den privaten Funkchannel weitergeleitet.

# 0.1.1

- Matrix-Raum-IDs ohne Server-Endung werden akzeptiert, einschließlich Raumversion 12.
- Hilfetexte zur internen Matrix-Raum-ID aktualisiert.

# 0.1.0

- Erste Version der bidirektionalen Matrix-Meshtastic-Bridge mit USB, TCP, Bluetooth und MQTT.
