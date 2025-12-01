# Changelog

## [Unreleced] - 2024-12-01

### Added
- Handle tables with null primary keys gracefully
- Skip CDC processing for tables without primary keys configured
- Mark null-PK files as completed to prevent retry loops
- Support for scheduled full load strategy (separate Lambda)

### Changed
- Lambda no longer throws error for tables with null/missing primary keys
- CDC files for null-PK tables are skipped and logged
- Full load files for null-PK tables are skipped (use scheduled Lambda instead)

### Fixed
- RuntimeError when tables_keys.json contains null values
- Lambda crash on tables without primary key configuration
- Support for 135 tables that don't have unique primary keys

### Technical Details
- Modified lambda/handler.py lines 723-732
- Added null key check before MERGE processing
- Enhanced logging for null-PK scenarios
- Returns 200 status (success) for skipped files instead of error

### Impact
- ~675 tables with primary keys: No change (CDC continues normally)
- ~135 tables with null primary keys: CDC gracefully skipped (no errors)
- Enables hybrid strategy: CDC for some tables, scheduled loads for others

