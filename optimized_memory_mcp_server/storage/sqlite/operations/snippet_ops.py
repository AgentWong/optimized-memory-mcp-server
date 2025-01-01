"""Code snippet operations for SQLite storage backend."""
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import hashlib
from difflib import unified_diff
from ..utils.sanitization import sanitize_input

class SnippetOperations:
    """Handles code snippet operations with pattern recognition."""
    
    def __init__(self, pool):
        self.pool = pool

    async def create_snippet(
        self,
        title: str,
        language: str,
        content: str,
        tags: Optional[List[str]] = None,
        entity_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a new code snippet with initial version."""
        similarity_hash = self._compute_similarity_hash(content)
        
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                # Create snippet
                cursor = await conn.execute(
                    """
                    INSERT INTO code_snippets (
                        title, language, content, tags, entity_name,
                        metadata, similarity_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    RETURNING *
                    """,
                    (
                        title,
                        language,
                        content,
                        ','.join(tags) if tags else None,
                        entity_name,
                        json.dumps(metadata) if metadata else None,
                        similarity_hash
                    )
                )
                snippet = dict(await cursor.fetchone())
                
                # Create initial version
                await conn.execute(
                    """
                    INSERT INTO snippet_versions (
                        snippet_id, content, version_number, commit_message
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (snippet['id'], content, 1, "Initial version")
                )
                
                # Detect and store patterns
                await self._analyze_patterns(conn, snippet['id'], content)
                
                return snippet

    async def update_snippet(
        self,
        snippet_id: int,
        content: str,
        commit_message: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Update snippet content and create new version."""
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                # Get current version
                cursor = await conn.execute(
                    """
                    SELECT content, MAX(version_number) as current_version 
                    FROM snippet_versions 
                    WHERE snippet_id = ?
                    GROUP BY snippet_id
                    """,
                    (snippet_id,)
                )
                current = await cursor.fetchone()
                if not current:
                    raise ValueError(f"Snippet {snippet_id} not found")
                
                # Generate diff
                diff = '\n'.join(unified_diff(
                    current['content'].splitlines(),
                    content.splitlines(),
                    fromfile='previous',
                    tofile='current',
                    lineterm=''
                ))
                
                # Create new version
                await conn.execute(
                    """
                    INSERT INTO snippet_versions (
                        snippet_id, content, version_number, 
                        commit_message, diff
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        snippet_id,
                        content,
                        current['current_version'] + 1,
                        commit_message,
                        diff
                    )
                )
                
                # Update main snippet
                similarity_hash = self._compute_similarity_hash(content)
                cursor = await conn.execute(
                    """
                    UPDATE code_snippets 
                    SET content = ?, 
                        similarity_hash = ?,
                        metadata = CASE 
                            WHEN ? IS NOT NULL THEN ?
                            ELSE metadata
                        END
                    WHERE id = ?
                    RETURNING *
                    """,
                    (
                        content,
                        similarity_hash,
                        json.dumps(metadata) if metadata else None,
                        json.dumps(metadata) if metadata else None,
                        snippet_id
                    )
                )
                snippet = dict(await cursor.fetchone())
                
                # Update pattern analysis
                await self._analyze_patterns(conn, snippet_id, content)
                
                return snippet

    async def get_snippet_history(
        self,
        snippet_id: int,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get version history of a snippet."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(
                """
                SELECT * FROM snippet_versions
                WHERE snippet_id = ?
                ORDER BY version_number DESC
                LIMIT ?
                """,
                (snippet_id, limit)
            )
            return [dict(row) for row in await cursor.fetchall()]

    async def find_similar_snippets(
        self,
        content: str,
        language: Optional[str] = None,
        min_confidence: float = 0.7
    ) -> List[Dict[str, Any]]:
        """Find similar snippets using pattern matching."""
        similarity_hash = self._compute_similarity_hash(content)
        
        async with self.pool.get_connection() as conn:
            # Query with optional language filter
            if language:
                cursor = await conn.execute(
                    """
                    SELECT *, 
                        similarity(similarity_hash, ?) as confidence
                    FROM code_snippets
                    WHERE language = ?
                    AND similarity(similarity_hash, ?) >= ?
                    ORDER BY confidence DESC
                    """,
                    (similarity_hash, language, similarity_hash, min_confidence)
                )
            else:
                cursor = await conn.execute(
                    """
                    SELECT *,
                        similarity(similarity_hash, ?) as confidence
                    FROM code_snippets
                    WHERE similarity(similarity_hash, ?) >= ?
                    ORDER BY confidence DESC
                    """,
                    (similarity_hash, similarity_hash, min_confidence)
                )
            
            return [dict(row) for row in await cursor.fetchall()]

    def _compute_similarity_hash(self, content: str) -> str:
        """Compute similarity hash for pattern matching."""
        # Normalize content
        normalized = '\n'.join(
            line.strip() for line in content.splitlines()
            if line.strip() and not line.strip().startswith('#')
        )
        return hashlib.sha256(normalized.encode()).hexdigest()

    async def _analyze_patterns(
        self,
        conn,
        snippet_id: int,
        content: str
    ) -> None:
        """Analyze and store code patterns."""
        # Extract patterns (simplified example)
        patterns = self._extract_patterns(content)
        
        for pattern in patterns:
            # Store pattern if new
            cursor = await conn.execute(
                """
                INSERT INTO snippet_patterns (
                    pattern_name, pattern_type, pattern_hash, description
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT (pattern_hash) DO UPDATE SET
                    occurrence_count = occurrence_count + 1
                RETURNING id
                """,
                (
                    pattern['name'],
                    pattern['type'],
                    pattern['hash'],
                    pattern['description']
                )
            )
            pattern_id = (await cursor.fetchone())['id']
            
            # Record pattern match
            await conn.execute(
                """
                INSERT INTO snippet_pattern_matches (
                    snippet_id, pattern_id, match_location, confidence_score
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT (snippet_id, pattern_id) DO UPDATE SET
                    match_location = ?,
                    confidence_score = ?
                """,
                (
                    snippet_id,
                    pattern_id,
                    json.dumps(pattern['locations']),
                    pattern['confidence'],
                    json.dumps(pattern['locations']),
                    pattern['confidence']
                )
            )

    def _extract_patterns(self, content: str) -> List[Dict[str, Any]]:
        """Extract code patterns from content."""
        # This is a placeholder for pattern recognition logic
        # In a real implementation, this would use more sophisticated analysis
        patterns = []
        
        # Example pattern detection (very simplified)
        lines = content.splitlines()
        for i, line in enumerate(lines):
            # Function definition pattern
            if line.strip().startswith('def '):
                patterns.append({
                    'name': 'function_definition',
                    'type': 'syntax',
                    'hash': hashlib.sha256(line.strip().encode()).hexdigest(),
                    'description': 'Function definition',
                    'locations': [i],
                    'confidence': 1.0
                })
            
            # Class definition pattern
            if line.strip().startswith('class '):
                patterns.append({
                    'name': 'class_definition',
                    'type': 'syntax',
                    'hash': hashlib.sha256(line.strip().encode()).hexdigest(),
                    'description': 'Class definition',
                    'locations': [i],
                    'confidence': 1.0
                })
        
        return patterns
