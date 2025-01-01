# File Size and Organization Guidelines

## Size Constraints

### Hard Limits
- Maximum file size: 250 lines of code (strict limit)
- Minimum recommended file size: 100 lines of code
- Target range: 100-250 lines per file

### Exceptions to Minimum Size
The following types of files may be less than 100 lines:
1. Configuration files (e.g., settings.py)
2. Package initialization files (__init__.py)
3. Interface definitions (e.g., abstract base classes)
4. Simple utility scripts with a single, focused purpose
5. Type definition files (e.g., custom types and dataclasses)

## File Consolidation Guidelines

### When to Consolidate Files
Consider consolidating multiple files when:
1. Files have high coupling (frequently import each other)
2. Functions operate on the same data structures
3. Classes share significant functionality
4. Related utility functions are spread across multiple files
5. Multiple files have similar prefixes/purposes
6. Total lines across related files is less than 250

### How to Consolidate Effectively
1. Group by Domain
   - Combine functions that work with the same domain objects
   - Keep business logic for a specific feature together
   - Maintain CRUD operations for a particular entity in one file

2. Group by Layer
   - Keep all repository methods for a domain together
   - Combine related service layer functions
   - Group validation logic for related entities

3. Use Classes to Organize Related Functions
   ```python
   class UserService:
       def create_user(self, data): ...
       def update_user(self, id, data): ...
       def delete_user(self, id): ...
       def validate_user_data(self, data): ...
   ```

4. Create Focused Utility Modules
   ```python
   # Instead of multiple small files:
   # string_utils.py (30 lines)
   # date_utils.py (40 lines)
   # number_utils.py (25 lines)
   
   # Consolidate into:
   # utils.py (~100 lines)
   class StringUtils:
       @staticmethod
       def normalize(s): ...
       
   class DateUtils:
       @staticmethod
       def format_iso(d): ...
       
   class NumberUtils:
       @staticmethod
       def round_currency(n): ...
   ```

## Anti-Patterns to Avoid

### Over-Fragmentation
```python
# Bad: Excessive fragmentation
# user_create.py
def create_user(data): ...

# user_update.py
def update_user(id, data): ...

# user_delete.py
def delete_user(id): ...

# Better: Consolidated in user_operations.py
class UserOperations:
    def create_user(self, data): ...
    def update_user(self, id, data): ...
    def delete_user(self, id): ...
    # Related helper methods
    def _validate_user_data(self, data): ...
    def _normalize_user_fields(self, data): ...
```

### Under-Consolidation
```python
# Bad: Related utilities scattered
# string_helpers.py (30 lines)
def normalize_string(s): ...
def capitalize_words(s): ...

# text_utils.py (25 lines)
def remove_special_chars(s): ...
def count_words(s): ...

# Better: Consolidated text_processing.py
class TextProcessor:
    @staticmethod
    def normalize(s): ...
    
    @staticmethod
    def capitalize_words(s): ...
    
    @staticmethod
    def remove_special_chars(s): ...
    
    @staticmethod
    def count_words(s): ...
    
    # Room for related functionality while staying under 250 lines
```

## Decision Framework

When deciding whether to split or consolidate files, ask:
1. Are the functions logically related?
2. Do they operate on the same data structures?
3. Are they likely to change together?
4. Would splitting make the code harder to understand?
5. Would consolidation make the file too complex?
6. Is the current organization making imports complicated?

### Example Decision Process
```python
# Scenario: Multiple small validator files

# Current structure:
# email_validator.py (40 lines)
# password_validator.py (35 lines)
# username_validator.py (30 lines)

# Decision process:
# 1. Related functionality? Yes - all validation
# 2. Similar patterns? Yes - all return bool/errors
# 3. Change together? Often - security updates
# 4. Total size? 105 lines - within limits
# 5. Clear organization? Yes - can use class structure

# Solution: Consolidate into validators.py
class UserValidators:
    @staticmethod
    def validate_email(email): ...
    
    @staticmethod
    def validate_password(password): ...
    
    @staticmethod
    def validate_username(username): ...
    
    # Room for related validators and shared utilities
```

## Implementation Strategy

When consolidating files:
1. Create a new file with a clear, encompassing name
2. Move related functions/classes into the new file
3. Update imports across the codebase
4. Add clear section comments to maintain organization
5. Consider using classes to group related functionality
6. Add comprehensive module-level documentation
7. Ensure the consolidated file stays under 250 lines

Remember: The goal is to balance maintainability with logical organization. Files should be neither too fragmented nor too monolithic.