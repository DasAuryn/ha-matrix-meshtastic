# Matrix Meshtastic Bridge

Dieses Add-on verbindet einen Matrix-Raum mit einem privaten Meshtastic-Channel in beide Richtungen. Es verwendet einen vorhandenen Matrix-Server. Weitere Add-ons werden nicht automatisch installiert.

## Einrichtung

Das Git-Repository lässt sich direkt in Home Assistant hinzufügen:

1. Unter **Einstellungen → Apps → App installieren** den App-Store öffnen.
2. Im Menü oben rechts **Repositories** auswählen.
3. Die HTTPS-URL dieses veröffentlichten Git-Repositories eintragen und hinzufügen.
4. **Matrix Meshtastic Bridge** im Abschnitt **Matrix Meshtastic Apps** öffnen und installieren.

Home Assistant baut die App bei der ersten Installation aus dem mitgelieferten Dockerfile. Unterstützt werden `aarch64` und `amd64`.

Für eine lokale Installation den Ordner `matrix_meshtastic` in das Add-on-Verzeichnis `/addons` von Home Assistant kopieren. Danach den Add-on-Store aktualisieren und „Matrix Meshtastic Bridge“ unter den lokalen Add-ons installieren.

1. Ein eigenes Matrix-Bot-Konto auf deinem Homeserver anlegen und in den gewünschten Raum einladen.
2. Unter `matrix` die Homeserver-Adresse, die vollständige `user_id` und die `room_id` eintragen. Alternativ zur Raum-ID ist ein Raum-Alias möglich.
3. Für die Anmeldung entweder `password` oder `access_token` zusammen mit dessen `device_id` eintragen.
4. Unter `meshtastic.connection` eine Verbindungsart auswählen und deren Felder ausfüllen.
5. `channel_index` auf den lokalen privaten Channel des Knotens setzen und dessen AES-Schlüssel als Base64 in `channel_psk` eintragen.
6. Das Add-on starten. Nachrichten im Matrix-Raum werden auf diesen Channel gesendet; Textnachrichten dieses Channels erscheinen über das Bot-Konto im selben Matrix-Raum.

Die Channel-Nummer von 0 bis 7 bezeichnet einen Slot auf dem Knoten. Channel-Name, Schlüssel und Funkkonfiguration müssen dort bereits eingerichtet sein. Die Bridge konfiguriert keine Channels auf dem Gerät.

## Private Channels

`channel_psk` muss einen eigenen AES-Schlüssel mit 16 oder 32 Bytes enthalten. Leere Schlüssel, Nullschlüssel, Ein-Byte-Aliase wie `AQ==` und bekannte öffentliche Standardschlüssel werden abgewiesen. Bei USB, TCP und Bluetooth liest die Bridge vor einer Weiterleitung die Channel-Konfiguration frisch vom Knoten und vergleicht den Schlüssel. Über MQTT müssen Gateway und Bridge manuell passend konfiguriert werden.

Die direkte Meshtastic-API bietet keinen atomaren Befehl für Schlüsselprüfung und anschließendes Senden. Die Channel-Konfiguration deshalb während des Bridge-Betriebs nicht über andere Clients ändern. Bei einer erkannten Abweichung stoppt die Bridge; sie legt keinen Ersatzchannel an und weicht nicht auf Channel 0 aus.

Der Schlüssel darf nicht als Beispielwert aus einer öffentlich zugänglichen Konfiguration übernommen werden. Nachrichtentexte und Schlüssel werden nicht in die Betriebsprotokolle geschrieben.

## Verbindungen

| Verbindung | Erforderliche Felder | Voraussetzung |
| --- | --- | --- |
| USB | `serial_device` | Knoten am Home-Assistant-Gerät, vorzugsweise mit stabilem Pfad unter `/dev/serial/by-id/` |
| TCP | `tcp_host`, `tcp_port` | Erreichbare Meshtastic-TCP-API; Standardport 4403 |
| Bluetooth | `ble_address` | Lokaler Bluetooth-Adapter und aktiviertes Bluetooth am Knoten |
| MQTT | `mqtt_host`, `mqtt_port`, `mqtt_topic`, `mqtt_channel_name`, `mqtt_gateway_id` | Gateway am selben Broker mit verschlüsseltem Protobuf-Uplink und -Downlink |

Es ist jeweils eine Verbindung aktiv. Unbenutzte Verbindungsfelder können leer bleiben. Die Bridge legt keinen MQTT-Broker an.

### Bluetooth

Das Add-on spricht über den Host-D-Bus mit BlueZ und verwendet einen lokalen Adapter. Home-Assistant- und ESPHome-Bluetooth-Proxys werden nicht eingebunden. Bei Knoten mit Bluetooth-PIN kann vorab eine Kopplung auf dem Host erforderlich sein. Eine verbundene Smartphone-App kann die Verbindung zum Knoten belegen.

Für USB und Bluetooth sind der Zugriff auf serielle Geräte und den Host-D-Bus im Add-on hinterlegt. Das Add-on startet keinen eigenen Bluetooth-Dienst.

### MQTT

Die Bridge überträgt AES-verschlüsselte Meshtastic-Protobuf-Pakete. JSON-Downlink wird nicht verwendet. Gateway und Add-on müssen denselben Broker, denselben privaten Channel und denselben Schlüssel nutzen.

Auf dem Gateway MQTT mit `encryption_enabled: true` aktivieren. Für den privaten Zielchannel müssen `uplink_enabled` und `downlink_enabled` eingeschaltet sein. `mqtt_channel_name` muss dessen exakter Name sein. `mqtt_topic` bezeichnet den Protobuf-Präfix einschließlich `/2/e`, zum Beispiel `msh/EU_868/2/e`. Das muss zum Root-Topic des Gateways passen.

Das Gateway muss MQTT-Pakete verarbeiten dürfen (`ignore_mqtt: false`). Sein Rebroadcast-Modus muss die virtuelle Bridge als Sender zulassen; `KNOWN_ONLY` kann Nachrichten einer noch unbekannten Bridge verwerfen.

`mqtt_gateway_id` enthält die tatsächliche Node-ID des Gateways, etwa `!abcd1234`. Empfangene Pakete werden auf dieses Gateway und den privaten Channel begrenzt. Für ausgehende Pakete verwendet die Bridge eine eigene dauerhaft gespeicherte virtuelle Node-ID. Sendepakete werden ohne Retain veröffentlicht.

Bei Bedarf `mqtt_username` und `mqtt_password` ergänzen. Für TLS `mqtt_tls` aktivieren und den passenden Port, üblicherweise 8883, wählen. Zertifikate werden geprüft. Die Gateway-ID ist ein Filter, kein kryptografischer Identitätsnachweis. Die Bridge kann die Gateway-Konfiguration oder eine tatsächliche Funkaussendung über MQTT nicht bestätigen. Siehe [Meshtastic-MQTT-Dokumentation](https://meshtastic.org/docs/software/integrations/mqtt/).

## Verschlüsselte Matrix-Räume

Matrix-Ende-zu-Ende-Verschlüsselung wird unterstützt. Gerätesitzung und Schlüssel bleiben unter `/data` erhalten und gehören zum Add-on-Backup. Andere Matrix-Geräte werden nicht automatisch als vertrauenswürdig eingestuft. Raumteilnehmer müssen dem Bot-Gerät bei Bedarf die Raum-Schlüssel freigeben. Eine interaktive Geräteverifikation bietet das Add-on derzeit nicht an. Ohne verfügbare Schlüssel kann eine Nachricht nicht entschlüsselt werden.

Die Bridge entschlüsselt Nachrichten zwischen den beiden Systemen. Für die Funkübertragung gilt der private Meshtastic-Schlüssel; die Matrix-Verschlüsselung reicht nicht bis zum Funkgerät.

## Weiterleitung und Betriebsgrenzen

Beim ersten Start wird der vorhandene Matrix-Nachrichtenverlauf übersprungen. Anschließend werden Nachrichten und Matrix-Fortschritt dauerhaft gespeichert. Event- und Paket-IDs sowie die eigene Bot- beziehungsweise Bridge-Identität begrenzen doppelte Weiterleitungen und Rückkopplungen.

`include_sender` stellt den Absender voran. `max_message_age_seconds` verwirft veraltete Nachrichten, auch nach einem Verbindungsproblem. `message_interval_seconds` legt die Mindestpause zwischen Funksendungen fest.

Meshtastic begrenzt die Nutzlast nach UTF-8-Bytes. Bei `long_message: split` werden lange Texte auf bis zu `max_parts` Pakete aufgeteilt; `truncate` kürzt auf ein Paket. Absender und Teilnummern zählen zur Paketgröße.

Eine erfolgreiche Übergabe an die Geräte-API oder den Broker bestätigt keine Funkzustellung. Bleibt der Ausgang eines begonnenen Sendeversuchs unklar, wird dieser nicht automatisch wiederholt. Eine lückenlose Zustellung wird nicht zugesichert.
