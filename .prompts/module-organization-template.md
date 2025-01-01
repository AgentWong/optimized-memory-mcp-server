# Python Module Organization Template

## Critical Requirements

1. This template is for ORGANIZATIONAL changes only. You must NOT:
   - Add new features or functionality
   - Modify existing business logic
   - Change the behavior of any code
   - Introduce new dependencies unless absolutely required for organization

2. Script files must not exceed 250 lines of code
   - This is a hard requirement
   - If reorganization would exceed this limit, the code must be split into multiple files
   - When splitting files, maintain existing functionality exactly as is

Current code structure:
[Describe or show current organization]

## Analysis Requirements

Please provide a detailed analysis and recommendations in the following format:

1. Module Structure Assessment:
   - Current file organization evaluation
   - Module dependency analysis
   - Interface clarity review
   - Identification of any violations of the 250-line limit

2. Code Segmentation Plan:
   - Proposed logical groupings of functions and classes
   - Clear rationale for any proposed file splits
   - Utility code identification and separation
   - Preservation of existing functionality

3. Implementation Approach:
   - Detailed steps for reorganization
   - Import statement restructuring
   - Documentation updates
   - Verification steps to ensure no functional changes

4. Risk Mitigation:
   - Potential impact assessment
   - Testing strategy for reorganization
   - Rollback plan if needed
   - Dependency management considerations

Remember:
- Focus exclusively on organization and structure
- Maintain existing functionality exactly
- Do not suggest feature improvements or enhancements
- Any changes must be purely organizational in nature