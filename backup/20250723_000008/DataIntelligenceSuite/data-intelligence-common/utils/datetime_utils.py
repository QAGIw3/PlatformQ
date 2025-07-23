"""
DateTime utilities.

Provides utilities for datetime handling, parsing, and timezone management.
"""

from typing import Optional, Union, List, Tuple
from datetime import datetime, date, time, timedelta, timezone
from dateutil import parser, tz, relativedelta
from dateutil.relativedelta import relativedelta as rd
import pytz
from enum import Enum
import re
from zoneinfo import ZoneInfo

from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class TimeUnit(str, Enum):
    """Time units"""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class DateFormat(str, Enum):
    """Common date formats"""
    ISO = "%Y-%m-%d"
    ISO_DATETIME = "%Y-%m-%d %H:%M:%S"
    ISO_DATETIME_TZ = "%Y-%m-%d %H:%M:%S%z"
    US = "%m/%d/%Y"
    US_DATETIME = "%m/%d/%Y %H:%M:%S"
    EU = "%d/%m/%Y"
    EU_DATETIME = "%d/%m/%Y %H:%M:%S"
    FILENAME = "%Y%m%d_%H%M%S"
    HUMAN = "%B %d, %Y"
    HUMAN_TIME = "%B %d, %Y at %I:%M %p"


class DateTimeParser:
    """Advanced datetime parsing"""
    
    # Common date patterns
    PATTERNS = [
        # ISO formats
        r'^\d{4}-\d{2}-\d{2}$',
        r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}',
        
        # US formats
        r'^\d{1,2}/\d{1,2}/\d{4}',
        r'^\d{1,2}-\d{1,2}-\d{4}',
        
        # EU formats
        r'^\d{1,2}\.\d{1,2}\.\d{4}',
        
        # Relative dates
        r'^(today|yesterday|tomorrow)',
        r'^\d+ (second|minute|hour|day|week|month|year)s? (ago|from now)',
        
        # Unix timestamps
        r'^\d{10}$',  # Seconds
        r'^\d{13}$',  # Milliseconds
    ]
    
    @staticmethod
    def parse(
        date_string: Union[str, int, float, datetime],
        timezone: Optional[str] = None,
        default_tz: Optional[str] = "UTC"
    ) -> datetime:
        """Parse various date formats"""
        if isinstance(date_string, datetime):
            dt = date_string
        elif isinstance(date_string, (int, float)):
            # Unix timestamp
            if date_string > 1e10:  # Milliseconds
                dt = datetime.fromtimestamp(date_string / 1000, tz=pytz.UTC)
            else:  # Seconds
                dt = datetime.fromtimestamp(date_string, tz=pytz.UTC)
        else:
            # Parse relative dates
            dt = DateTimeParser._parse_relative(date_string)
            if dt:
                return dt
                
            # Use dateutil parser
            try:
                dt = parser.parse(date_string)
            except Exception as e:
                raise ValueError(f"Cannot parse date: {date_string}") from e
                
        # Apply timezone
        if timezone:
            if dt.tzinfo is None:
                # Assume default timezone
                default_tz_obj = pytz.timezone(default_tz)
                dt = default_tz_obj.localize(dt)
            # Convert to target timezone
            target_tz = pytz.timezone(timezone)
            dt = dt.astimezone(target_tz)
        elif dt.tzinfo is None and default_tz:
            # Apply default timezone
            default_tz_obj = pytz.timezone(default_tz)
            dt = default_tz_obj.localize(dt)
            
        return dt
        
    @staticmethod
    def _parse_relative(date_string: str) -> Optional[datetime]:
        """Parse relative date strings"""
        date_string = date_string.lower().strip()
        now = datetime.now(pytz.UTC)
        
        # Simple relative dates
        if date_string == "today":
            return now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif date_string == "yesterday":
            return (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        elif date_string == "tomorrow":
            return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        elif date_string == "now":
            return now
            
        # Relative time expressions
        match = re.match(r'^(\d+) (second|minute|hour|day|week|month|year)s? (ago|from now)$', date_string)
        if match:
            amount = int(match.group(1))
            unit = match.group(2)
            direction = match.group(3)
            
            if unit in ["second", "minute", "hour", "day", "week"]:
                # Use timedelta
                delta_args = {f"{unit}s": amount}
                delta = timedelta(**delta_args)
            else:
                # Use relativedelta for months/years
                delta_args = {f"{unit}s": amount}
                delta = rd(**delta_args)
                
            if direction == "ago":
                return now - delta
            else:
                return now + delta
                
        return None


class TimeZoneUtils:
    """Timezone utilities"""
    
    @staticmethod
    def get_timezone_offset(timezone_name: str, dt: Optional[datetime] = None) -> timedelta:
        """Get timezone offset from UTC"""
        if dt is None:
            dt = datetime.now()
            
        tz = pytz.timezone(timezone_name)
        
        # Localize to handle DST
        if dt.tzinfo is None:
            dt = tz.localize(dt)
        else:
            dt = dt.astimezone(tz)
            
        return dt.utcoffset()
        
    @staticmethod
    def convert_timezone(
        dt: datetime,
        from_tz: Union[str, timezone],
        to_tz: Union[str, timezone]
    ) -> datetime:
        """Convert datetime between timezones"""
        # Convert timezone arguments
        if isinstance(from_tz, str):
            from_tz = pytz.timezone(from_tz)
        if isinstance(to_tz, str):
            to_tz = pytz.timezone(to_tz)
            
        # Localize if naive
        if dt.tzinfo is None:
            dt = from_tz.localize(dt)
        else:
            dt = dt.astimezone(from_tz)
            
        # Convert to target timezone
        return dt.astimezone(to_tz)
        
    @staticmethod
    def list_timezones(pattern: Optional[str] = None) -> List[str]:
        """List available timezones"""
        timezones = pytz.all_timezones
        
        if pattern:
            pattern_lower = pattern.lower()
            timezones = [tz for tz in timezones if pattern_lower in tz.lower()]
            
        return sorted(timezones)
        
    @staticmethod
    def get_local_timezone() -> str:
        """Get local system timezone"""
        return str(tz.tzlocal())


class DateRangeUtils:
    """Date range utilities"""
    
    @staticmethod
    def get_range(
        unit: TimeUnit,
        start: Optional[datetime] = None,
        periods: int = 1,
        inclusive: bool = True
    ) -> Tuple[datetime, datetime]:
        """Get date range for time unit"""
        if start is None:
            start = datetime.now(pytz.UTC)
            
        # Ensure timezone
        if start.tzinfo is None:
            start = pytz.UTC.localize(start)
            
        # Calculate range based on unit
        if unit == TimeUnit.SECOND:
            start_range = start.replace(microsecond=0)
            end_range = start_range + timedelta(seconds=periods)
        elif unit == TimeUnit.MINUTE:
            start_range = start.replace(second=0, microsecond=0)
            end_range = start_range + timedelta(minutes=periods)
        elif unit == TimeUnit.HOUR:
            start_range = start.replace(minute=0, second=0, microsecond=0)
            end_range = start_range + timedelta(hours=periods)
        elif unit == TimeUnit.DAY:
            start_range = start.replace(hour=0, minute=0, second=0, microsecond=0)
            end_range = start_range + timedelta(days=periods)
        elif unit == TimeUnit.WEEK:
            # Start of week (Monday)
            days_since_monday = start.weekday()
            start_range = start.replace(hour=0, minute=0, second=0, microsecond=0)
            start_range = start_range - timedelta(days=days_since_monday)
            end_range = start_range + timedelta(weeks=periods)
        elif unit == TimeUnit.MONTH:
            start_range = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            end_range = start_range + rd(months=periods)
        elif unit == TimeUnit.QUARTER:
            # Start of quarter
            quarter = (start.month - 1) // 3
            start_month = quarter * 3 + 1
            start_range = start.replace(month=start_month, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_range = start_range + rd(months=3 * periods)
        elif unit == TimeUnit.YEAR:
            start_range = start.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_range = start_range + rd(years=periods)
        else:
            raise ValueError(f"Unknown time unit: {unit}")
            
        if not inclusive:
            end_range = end_range - timedelta(microseconds=1)
            
        return start_range, end_range
        
    @staticmethod
    def split_range(
        start: datetime,
        end: datetime,
        unit: TimeUnit
    ) -> List[Tuple[datetime, datetime]]:
        """Split date range into periods"""
        ranges = []
        current = start
        
        while current < end:
            period_start = current
            _, period_end = DateRangeUtils.get_range(unit, current, 1, True)
            
            if period_end > end:
                period_end = end
                
            ranges.append((period_start, period_end))
            current = period_end
            
        return ranges


class DurationUtils:
    """Duration calculation utilities"""
    
    @staticmethod
    def humanize(
        duration: Union[timedelta, int, float],
        precision: int = 2
    ) -> str:
        """Convert duration to human-readable format"""
        if isinstance(duration, (int, float)):
            duration = timedelta(seconds=duration)
            
        total_seconds = int(duration.total_seconds())
        
        if total_seconds < 0:
            return f"-{DurationUtils.humanize(timedelta(seconds=-total_seconds), precision)}"
            
        units = [
            ("year", 365 * 24 * 3600),
            ("month", 30 * 24 * 3600),
            ("week", 7 * 24 * 3600),
            ("day", 24 * 3600),
            ("hour", 3600),
            ("minute", 60),
            ("second", 1)
        ]
        
        parts = []
        remaining = total_seconds
        
        for unit_name, unit_seconds in units:
            if remaining >= unit_seconds:
                count = remaining // unit_seconds
                remaining = remaining % unit_seconds
                
                if count == 1:
                    parts.append(f"{count} {unit_name}")
                else:
                    parts.append(f"{count} {unit_name}s")
                    
                if len(parts) >= precision:
                    break
                    
        if not parts:
            return "0 seconds"
            
        if len(parts) == 1:
            return parts[0]
        else:
            return ", ".join(parts[:-1]) + f" and {parts[-1]}"
            
    @staticmethod
    def parse_duration(duration_str: str) -> timedelta:
        """Parse duration string to timedelta"""
        # Handle ISO 8601 duration
        if duration_str.startswith("P"):
            return DurationUtils._parse_iso_duration(duration_str)
            
        # Parse human-readable format
        pattern = r'(\d+)\s*(second|minute|hour|day|week|month|year)s?'
        matches = re.findall(pattern, duration_str.lower())
        
        if not matches:
            raise ValueError(f"Cannot parse duration: {duration_str}")
            
        total_seconds = 0
        
        for amount, unit in matches:
            amount = int(amount)
            
            if unit == "second":
                total_seconds += amount
            elif unit == "minute":
                total_seconds += amount * 60
            elif unit == "hour":
                total_seconds += amount * 3600
            elif unit == "day":
                total_seconds += amount * 86400
            elif unit == "week":
                total_seconds += amount * 604800
            elif unit == "month":
                total_seconds += amount * 2592000  # 30 days
            elif unit == "year":
                total_seconds += amount * 31536000  # 365 days
                
        return timedelta(seconds=total_seconds)
        
    @staticmethod
    def _parse_iso_duration(duration_str: str) -> timedelta:
        """Parse ISO 8601 duration"""
        # Simple ISO 8601 parser (not complete)
        pattern = r'P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?'
        match = re.match(pattern, duration_str)
        
        if not match:
            raise ValueError(f"Invalid ISO duration: {duration_str}")
            
        years, months, days, hours, minutes, seconds = match.groups()
        
        total_days = 0
        if years:
            total_days += int(years) * 365
        if months:
            total_days += int(months) * 30
        if days:
            total_days += int(days)
            
        total_seconds = 0
        if hours:
            total_seconds += int(hours) * 3600
        if minutes:
            total_seconds += int(minutes) * 60
        if seconds:
            total_seconds += float(seconds)
            
        return timedelta(days=total_days, seconds=total_seconds)


class BusinessDateUtils:
    """Business date calculations"""
    
    # Default holidays (US federal holidays)
    DEFAULT_HOLIDAYS = [
        "New Year's Day",
        "Martin Luther King Jr. Day",
        "Presidents Day",
        "Memorial Day",
        "Independence Day",
        "Labor Day",
        "Columbus Day",
        "Veterans Day",
        "Thanksgiving",
        "Christmas Day"
    ]
    
    @staticmethod
    def is_business_day(
        dt: Union[datetime, date],
        holidays: Optional[List[Union[datetime, date]]] = None
    ) -> bool:
        """Check if date is a business day"""
        if isinstance(dt, datetime):
            dt = dt.date()
            
        # Check weekend
        if dt.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
            
        # Check holidays
        if holidays:
            holiday_dates = [h.date() if isinstance(h, datetime) else h for h in holidays]
            if dt in holiday_dates:
                return False
                
        return True
        
    @staticmethod
    def add_business_days(
        start_date: Union[datetime, date],
        days: int,
        holidays: Optional[List[Union[datetime, date]]] = None
    ) -> Union[datetime, date]:
        """Add business days to date"""
        is_datetime = isinstance(start_date, datetime)
        current = start_date
        
        days_added = 0
        direction = 1 if days >= 0 else -1
        days = abs(days)
        
        while days_added < days:
            current = current + timedelta(days=direction)
            if BusinessDateUtils.is_business_day(current, holidays):
                days_added += 1
                
        return current
        
    @staticmethod
    def business_days_between(
        start_date: Union[datetime, date],
        end_date: Union[datetime, date],
        holidays: Optional[List[Union[datetime, date]]] = None
    ) -> int:
        """Count business days between dates"""
        if isinstance(start_date, datetime):
            start_date = start_date.date()
        if isinstance(end_date, datetime):
            end_date = end_date.date()
            
        if start_date > end_date:
            start_date, end_date = end_date, start_date
            
        count = 0
        current = start_date
        
        while current <= end_date:
            if BusinessDateUtils.is_business_day(current, holidays):
                count += 1
            current = current + timedelta(days=1)
            
        return count


# Convenience functions

def now(timezone: Optional[str] = "UTC") -> datetime:
    """Get current datetime with timezone"""
    tz = pytz.timezone(timezone) if timezone else pytz.UTC
    return datetime.now(tz)


def today(timezone: Optional[str] = "UTC") -> date:
    """Get today's date"""
    return now(timezone).date()


def parse_datetime(
    date_string: Union[str, int, float, datetime],
    timezone: Optional[str] = None
) -> datetime:
    """Parse datetime string"""
    return DateTimeParser.parse(date_string, timezone)


def format_datetime(
    dt: datetime,
    format: Union[str, DateFormat] = DateFormat.ISO_DATETIME
) -> str:
    """Format datetime"""
    if isinstance(format, DateFormat):
        format = format.value
    return dt.strftime(format)


def humanize_duration(duration: Union[timedelta, int, float]) -> str:
    """Convert duration to human-readable format"""
    return DurationUtils.humanize(duration) 