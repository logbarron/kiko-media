import Foundation
import OSLog

package enum DateUtils {
    nonisolated(unsafe) private static var _calendar: Calendar?

    package static func configure(eventTimezone: String) {
        var cal = Calendar(identifier: .gregorian)
        let systemTZ = TimeZone.current
        let tzName = eventTimezone.trimmingCharacters(in: .whitespacesAndNewlines)

        if tzName.isEmpty {
            cal.timeZone = systemTZ
            Logger.kiko.info("Event timezone: \(systemTZ.identifier) (system default)")
        } else if let tz = TimeZone(identifier: tzName) {
            cal.timeZone = tz
            Logger.kiko.info("Event timezone: \(tzName) (from EVENT_TIMEZONE)")
        } else {
            cal.timeZone = systemTZ
            Logger.kiko.warning("EVENT_TIMEZONE '\(tzName)' is not a valid IANA identifier, using system default: \(systemTZ.identifier)")
        }

        _calendar = cal
    }

    private static var calendar: Calendar {
        guard let cal = _calendar else {
            preconditionFailure("DateUtils.configure() must be called before use")
        }
        return cal
    }

    package static func exifTimestamp(from date: Date) -> String {
        let c = calendar.dateComponents(
            [.year, .month, .day, .hour, .minute, .second], from: date)
        guard let y = c.year, let mo = c.month, let d = c.day,
              let h = c.hour, let mi = c.minute, let s = c.second else {
            return "0000:00:00 00:00:00"
        }
        return String(format: "%04d:%02d:%02d %02d:%02d:%02d", y, mo, d, h, mi, s)
    }
}
