# Matrix Meshtastic Bridge

Diese App ist ein eigenständiger Matrix-Messenger mit einer optionalen bidirektionalen Meshtastic-Bridge. Sie verwendet einen vorhandenen Matrix-Server. Matrix funktioniert auch ohne Funkgerät, MQTT-Broker oder Meshtastic-Schlüssel.

## Einrichtung

Das Git-Repository lässt sich direkt in Home Assistant hinzufügen:

1. Unter **Einstellungen → Apps → App installieren** den App-Store öffnen.
2. Im Menü oben rechts **Repositories** auswählen.
3. Die HTTPS-URL dieses veröffentlichten Git-Repositories eintragen und hinzufügen.
4. **Matrix Meshtastic Bridge** im Abschnitt **Matrix Meshtastic Apps** öffnen und installieren.

Home Assistant baut die App bei der ersten Installation aus dem mitgelieferten Dockerfile. Unterstützt werden `aarch64` und `amd64`.

Für eine lokale Installation den Ordner `matrix_meshtastic` in das Add-on-Verzeichnis `/addons` von Home Assistant kopieren. Danach den Add-on-Store aktualisieren und „Matrix Meshtastic Bridge“ unter den lokalen Add-ons installieren.

## Matrix-Messenger ohne Meshtastic

1. Die App starten und **Web-Oberfläche öffnen** auswählen. Die Oberfläche startet auch ohne vorher eingetragene Zugangsdaten.
2. Unter **Einstellungen → Matrix-Konto** die Serveradresse, die vollständige Matrix-ID und das Passwort eintragen und **Speichern & verbinden** wählen. Einrichtungsbeispiel: Server `https://matrix.org`, Matrix-ID `@name:matrix.org`.
3. Falls bereits ein Zugangstoken vorhanden ist, stehen Access-Token und zugehörige Geräte-ID unter **Erweiterte Anmeldung** zur Verfügung.
4. Meshtastic ausgeschaltet lassen. Ein Funkgerät oder Funkschlüssel ist für den Messenger nicht erforderlich.

Vorhandene App-Optionen werden bei der ersten Einrichtung übernommen. Über Ingress gespeicherte Einstellungen haben anschließend Vorrang und bleiben bei Neustarts und Updates erhalten. Änderungen werden übernommen, ohne die Ingress-Oberfläche neu zu starten. Bereits gespeicherte Passwörter und Schlüssel werden nicht zurück an den Browser übertragen; leere Geheimnisfelder behalten den vorhandenen Wert. Ein Kontowechsel übernimmt keine Zugangsdaten des vorherigen Kontos.

Über **Abmelden** kann die aktuelle Anmeldung ausdrücklich beendet werden. Importierte lokale Schlüssel bleiben geschützt erhalten. Wenn die Server-Abmeldung wegen einer Verbindungsstörung nicht bestätigt werden kann, weist die Oberfläche darauf hin.

Der Messenger zeigt die Räume, Direktnachrichten und Einladungen des angemeldeten Kontos. Nachrichten lassen sich lesen und schreiben, ältere Nachrichten nachladen, Einladungen annehmen und Direktnachrichten über eine Matrix-Benutzer-ID beginnen. Bestehende Nachrichten werden zur Anzeige synchronisiert, aber beim ersten Start nicht nachträglich ins Funknetz übertragen.

Unter **Räume verwalten** lassen sich neue Räume erstellen und vorhandene Räume über Raum-ID oder Alias öffnen. Die Verwaltung eines geöffneten Raums bietet Umbenennen, Einladen und bestätigtes Verlassen. Diese Aktionen benötigen die jeweiligen Berechtigungen des Matrix-Kontos.

Chatverlauf und Geräteschlüssel bleiben unter `/data` gespeichert und sind Teil des App-Backups. Bei einer Unterbrechung bleibt der vorhandene lokale Verlauf sichtbar. Ein Wechsel des Matrix-Kontos trennt dessen Chatdaten vom vorherigen Konto. Zugangstoken und Schlüssel werden nicht an den Browser ausgegeben. Die Oberfläche ist ausschließlich über den authentifizierten Home-Assistant-Ingress zugänglich.

## Optionale Meshtastic-Bridge

1. In **Einstellungen → Meshtastic** die Bridge aktivieren.
2. Den Matrix-Raum für die Funkweiterleitung aus der Liste wählen. Alternativ kann die interne Raum-ID oder ein Alias manuell eingetragen werden.
3. Die Verbindung auswählen und die angezeigten Felder ausfüllen. Bei USB können verfügbare serielle Geräte gesucht werden.
4. Den Channel-Slot und den privaten AES-Schlüssel des Funkgeräts eintragen. Die Oberfläche kann einen neuen privaten Schlüssel erzeugen; derselbe Schlüssel muss anschließend auch auf dem Funkgerät eingerichtet werden.
5. **Speichern & verbinden** wählen. Nur der ausgewählte Matrix-Raum wird mit diesem Funkchannel verbunden. Andere Räume und Direktnachrichten bleiben im Matrix-Messenger.

Ein fehlender oder unzulässiger Schlüssel sperrt nur die Funkweiterleitung. Ein nicht verbundenes Funkgerät wird erneut verbunden, während Matrix und Ingress weiterlaufen. Beide Verbindungszustände werden in der Oberfläche angezeigt. Nach einem Update von 0.1.x ist Meshtastic standardmäßig ausgeschaltet und muss bei Bedarf ausdrücklich aktiviert werden.

Die Channel-Nummer von 0 bis 7 bezeichnet einen Slot auf dem Knoten. Channel-Name, Schlüssel und Funkkonfiguration müssen dort bereits eingerichtet sein. Die Bridge konfiguriert keine Channels auf dem Gerät.

## Private Channels

`channel_psk` muss einen eigenen AES-Schlüssel mit 16 oder 32 Bytes enthalten. Leere Schlüssel, Nullschlüssel, Ein-Byte-Aliase wie `AQ==` und bekannte öffentliche Standardschlüssel werden abgewiesen. Bei USB, TCP und Bluetooth liest die Bridge vor einer Weiterleitung die Channel-Konfiguration frisch vom Knoten und vergleicht den Schlüssel. Über MQTT müssen Gateway und Bridge manuell passend konfiguriert werden.

Die direkte Meshtastic-API bietet keinen atomaren Befehl für Schlüsselprüfung und anschließendes Senden. Die Channel-Konfiguration deshalb während des Bridge-Betriebs nicht über andere Clients ändern. Bei einer erkannten Abweichung stoppt die Funkweiterleitung; der Matrix-Messenger läuft weiter. Es wird kein Ersatzchannel angelegt und nicht auf Channel 0 ausgewichen.

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

Matrix-Ende-zu-Ende-Verschlüsselung wird unterstützt. Gerätesitzung und Schlüssel bleiben unter `/data` erhalten und gehören zum App-Backup. Unter **Geräte & Schlüssel** können bekannte Geräte zur Verifikation ausgewählt oder eingehende Verifikationsanfragen angenommen werden. Die angezeigten Emojis mit dem anderen Gerät vergleichen und nur bei Übereinstimmung auf beiden Geräten bestätigen. Andere Geräte werden nicht automatisch als vertrauenswürdig eingestuft.

Wenn der bisherige Client den Symbolvergleich nicht anbietet, kann der vollständige Geräte-Fingerabdruck in dessen Geräteinformationen verglichen und anschließend in Ingress ausdrücklich bestätigt werden. Der gespeicherte Fingerabdruck wird vor der Bestätigung erneut geprüft. Nicht bestätigte Geräte können den Versand verschlüsselter Nachrichten blockieren; die Oberfläche zeigt dafür eine konkrete Aktion an.

Raumteilnehmer müssen dem verwendeten Gerät bei Bedarf die Raum-Schlüssel freigeben. Noch nicht entschlüsselbare Nachrichten werden als solche dargestellt und bei verfügbaren Schlüsseln erneut entschlüsselt. Ohne verfügbare Schlüssel ist deren Inhalt nicht lesbar. Der Messenger zeigt in dieser Version Textnachrichten; Datei- und Medienversand ist nicht enthalten.

### Element-Schlüsseldatei importieren

1. Im bisherigen Matrix-Client, etwa Element, die Raumschlüssel als passwortgeschützte Datei exportieren. Das Exportpasswort wird dabei selbst gewählt.
2. In Ingress **Geräte & Schlüssel → Schlüssel importieren** öffnen.
3. Die exportierte Datei, beispielsweise `elements-keys.txt`, auswählen und die beim Export gewählte Passphrase eingeben.
4. **Schlüssel importieren** wählen. Bereits empfangene Nachrichten werden mit den importierten Schlüsseln erneut entschlüsselt, ohne alte Nachrichten ins Funknetz zu übertragen.

Unterstützt wird der verschlüsselte Megolm-Sitzungsexport mit bis zu 10 MB. Ein Matrix-Backup-Wiederherstellungsschlüssel ist ein anderes Format. Die Passphrase wird nicht gespeichert. Die temporäre Importdatei wird nach Erfolg oder Fehler entfernt; die importierten Sitzungen bleiben im geschützten lokalen Schlüsselspeicher erhalten.

Die Bridge entschlüsselt Nachrichten zwischen den beiden Systemen. Für die Funkübertragung gilt der private Meshtastic-Schlüssel; die Matrix-Verschlüsselung reicht nicht bis zum Funkgerät.

## Weiterleitung und Betriebsgrenzen

Beim ersten Start wird der vorhandene Matrix-Nachrichtenverlauf übersprungen. Anschließend werden Nachrichten und Matrix-Fortschritt dauerhaft gespeichert. Event- und Paket-IDs sowie die eigene Bot- beziehungsweise Bridge-Identität begrenzen doppelte Weiterleitungen und Rückkopplungen.

`include_sender` stellt den Absender voran. `max_message_age_seconds` verwirft veraltete Nachrichten, auch nach einem Verbindungsproblem. `message_interval_seconds` legt die Mindestpause zwischen Funksendungen fest.

Meshtastic begrenzt die Nutzlast nach UTF-8-Bytes. Bei `long_message: split` werden lange Texte auf bis zu `max_parts` Pakete aufgeteilt; `truncate` kürzt auf ein Paket. Absender und Teilnummern zählen zur Paketgröße.

Eine erfolgreiche Übergabe an die Geräte-API oder den Broker bestätigt keine Funkzustellung. Bleibt der Ausgang eines begonnenen Sendeversuchs unklar, wird dieser nicht automatisch wiederholt. Eine lückenlose Zustellung wird nicht zugesichert.
