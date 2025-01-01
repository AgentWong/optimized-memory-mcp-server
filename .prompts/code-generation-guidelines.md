# Python Code Generation Guidelines for AI

## Core Requirements

1. All Python script files must be less than 250 lines of code. This is a strict requirement that promotes maintainability and forces proper separation of concerns. When a file approaches this limit, it should be split into logical components with clear responsibilities and interfaces.

## Code Organization Principles

Code should be organized to maximize readability, maintainability, and reusability. Each module should have a single, well-defined purpose that is clearly expressed through its organization and documentation.

### File Structure
Each Python file should follow a consistent structure with clear sections:

At the top of the file:
1. Module docstring describing purpose and usage
2. Future imports if needed
3. Standard library imports
4. Third-party package imports
5. Local application imports
6. Module-level constants and configurations

In the main body:
1. Class definitions
2. Function definitions
3. Main execution code (if applicable)

### Code Modularity

Functions and classes should be designed with clear boundaries and single responsibilities. Complex operations should be broken down into smaller, manageable components that can be understood, tested, and maintained independently.

### Interface Design

Public interfaces should be clearly defined and documented. Implementation details should be appropriately encapsulated using Python's naming conventions (_single_underscore for internal use, __double_underscore for name mangling).

## Documentation Standards

Documentation is an integral part of code design and should be written alongside the code, not as an afterthought.

### Module Documentation
Each module must include a docstring that explains:
1. The module's purpose
2. Key classes and functions
3. Usage examples
4. Any important notes or caveats

### Function and Class Documentation
All public functions and classes must have docstrings that specify:
1. Purpose and behavior
2. Parameters and their types
3. Return values and types
4. Exceptions that may be raised
5. Usage examples for complex interfaces

## Error Handling

Error handling should be comprehensive but appropriate to the context:
1. Use specific exception types rather than catching Exception
2. Provide meaningful error messages
3. Maintain appropriate exception hierarchy
4. Document error conditions and handling

## Testing Considerations

Code should be designed with testability in mind:
1. Functions should have clear inputs and outputs
2. Side effects should be minimized and well-documented
3. Complex conditions should be separated into testable components
4. Test hooks should be provided where appropriate

## Performance Guidelines

Performance considerations should be balanced with code clarity:
1. Optimize for readability first
2. Document performance characteristics
3. Use appropriate data structures
4. Consider memory usage
5. Profile before optimizing

## Security Guidelines

Security should be considered at every stage of development:
1. Input validation for all external data
2. Proper handling of sensitive information
3. Secure defaults for all configurable options
4. Regular security updates for dependencies

## Maintenance Considerations

Code should be written with future maintenance in mind:
1. Clear and consistent naming conventions
2. Regular refactoring to maintain code quality
3. Comprehensive documentation updates
4. Version control best practices

## Version Control Integration

Code should be organized to work effectively with version control:
1. Logical commit boundaries
2. Clear commit messages
3. Appropriate use of branching
4. Effective merge conflict resolution

## Dependency Management

Dependencies should be managed carefully:
1. Clear documentation of requirements
2. Version pinning for stability
3. Regular updates and security patches
4. Minimal dependency principle

## Code Review Guidelines

Code should be written with review in mind:
1. Clear and logical organization
2. Self-documenting where possible
3. Complex sections explained in comments
4. Consistent style and formatting

## Deployment Considerations

Code should be designed for reliable deployment:
1. Environment independence
2. Configuration management
3. Logging and monitoring hooks
4. Graceful error handling

## Backward Compatibility

Changes should maintain compatibility where possible:
1. Clear deprecation warnings
2. Migration guides for breaking changes
3. Version numbering adherence
4. Compatibility testing

These guidelines should be applied thoughtfully, with consideration for the specific context and requirements of each project. While they provide a framework for high-quality code, they should not be followed so rigidly as to impede practical development needs.