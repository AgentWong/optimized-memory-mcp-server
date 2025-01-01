"""Conversation operations for SQLite storage backend."""
from typing import List, Dict, Any, Optional, Set
from datetime import datetime
import json
import re
from collections import Counter
from ..utils.sanitization import sanitize_input

class ConversationOperations:
    """Handles conversation-related database operations with relevance scoring."""
    """Handles conversation-related database operations."""
    
    def __init__(self, pool):
        self.pool = pool
        
    async def create_conversation(
        self,
        title: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a new conversation."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO conversations (title, metadata)
                VALUES (?, ?)
                RETURNING *
                """,
                (title, json.dumps(metadata) if metadata else None)
            )
            row = await cursor.fetchone()
            return dict(row)
            
    async def add_message(
        self,
        conversation_id: int,
        message_type: str,
        content: str,
        entities_mentioned: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Add a message to a conversation and update relevance."""
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                # Insert message
                cursor = await conn.execute(
                    """
                    INSERT INTO conversation_messages (
                        conversation_id, message_type, content,
                        entities_mentioned, metadata
                    )
                    VALUES (?, ?, ?, ?, ?)
                    RETURNING *
                    """,
                    (
                        conversation_id,
                        message_type,
                        content,
                        ','.join(entities_mentioned) if entities_mentioned else None,
                        json.dumps(metadata) if metadata else None
                    )
                )
                message = dict(await cursor.fetchone())
                
                # Update conversation relevance
                await self.update_conversation_relevance(
                    conversation_id,
                    content=content,
                    context_entities=entities_mentioned
                )
                
                return message
            
    async def get_conversation(self, conversation_id: int) -> Dict[str, Any]:
        """Get conversation details with messages."""
        async with self.pool.get_connection() as conn:
            # Get conversation
            cursor = await conn.execute(
                "SELECT * FROM conversations WHERE id = ?",
                (conversation_id,)
            )
            conv_row = await cursor.fetchone()
            if not conv_row:
                return None
                
            conversation = dict(conv_row)
            
            # Get messages
            cursor = await conn.execute(
                """
                SELECT * FROM conversation_messages 
                WHERE conversation_id = ?
                ORDER BY timestamp ASC
                """,
                (conversation_id,)
            )
            messages = [dict(row) for row in await cursor.fetchall()]
            conversation['messages'] = messages
            
            return conversation
            
    async def calculate_message_relevance(
        self,
        content: str,
        entity_names: Set[str],
        context_weight: float = 0.6,
        recency_weight: float = 0.4
    ) -> float:
        """Calculate relevance score for a message based on entity mentions and context.
        
        Args:
            content: Message content
            entity_names: Set of known entity names
            context_weight: Weight for context relevance (0-1)
            recency_weight: Weight for temporal relevance (0-1)
            
        Returns:
            float: Relevance score between 0-1
        """
        # Normalize content
        normalized_content = content.lower()
        words = set(re.findall(r'\w+', normalized_content))
        
        # Calculate entity mention score
        mentioned_entities = {
            name for name in entity_names 
            if name.lower() in normalized_content
        }
        entity_score = len(mentioned_entities) / max(len(entity_names), 1)
        
        # Calculate context relevance using word frequency
        word_freq = Counter(words)
        context_score = min(
            sum(word_freq.values()) / max(len(normalized_content.split()), 1),
            1.0
        )
        
        # Combine scores with weights
        relevance_score = (
            context_weight * context_score +
            recency_weight * entity_score
        )
        
        return min(max(relevance_score, 0.0), 1.0)

    async def update_conversation_relevance(
        self,
        conversation_id: int,
        content: str = None,
        context_entities: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Update conversation relevance based on content and context.
        
        Args:
            conversation_id: ID of conversation to update
            content: Optional new message content to consider
            context_entities: Optional list of relevant entity names
            
        Returns:
            Dict containing updated conversation data
        """
        async with self.pool.get_connection() as conn:
            # Get existing entities if none provided
            if context_entities is None:
                cursor = await conn.execute(
                    "SELECT context_entities FROM conversations WHERE id = ?",
                    (conversation_id,)
                )
                row = await cursor.fetchone()
                if row and row['context_entities']:
                    context_entities = row['context_entities'].split(',')
                else:
                    context_entities = []

            # Calculate new relevance score
            if content:
                relevance_score = await self.calculate_message_relevance(
                    content,
                    set(context_entities)
                )
            else:
                # Get recent messages for scoring
                cursor = await conn.execute(
                    """
                    SELECT content FROM conversation_messages
                    WHERE conversation_id = ?
                    ORDER BY timestamp DESC LIMIT 5
                    """,
                    (conversation_id,)
                )
                messages = await cursor.fetchall()
                if messages:
                    # Average relevance of recent messages
                    scores = [
                        await self.calculate_message_relevance(
                            msg['content'],
                            set(context_entities)
                        )
                        for msg in messages
                    ]
                    relevance_score = sum(scores) / len(scores)
                else:
                    relevance_score = 0.5  # Default score for empty conversations

            # Update conversation
            cursor = await conn.execute(
                """
                UPDATE conversations
                SET relevance_score = ?,
                    context_entities = ?
                WHERE id = ?
                RETURNING *
                """,
                (
                    relevance_score,
                    ','.join(context_entities) if context_entities else None,
                    conversation_id
                )
            )
            row = await cursor.fetchone()
            return dict(row) if row else None
            
    async def get_recent_conversations(
        self,
        limit: int = 10,
        min_relevance: float = 0.0
    ) -> List[Dict[str, Any]]:
        """Get recent conversations with optional relevance filter."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(
                """
                SELECT * FROM conversations
                WHERE relevance_score >= ?
                ORDER BY last_message_time DESC
                LIMIT ?
                """,
                (min_relevance, limit)
            )
            return [dict(row) for row in await cursor.fetchall()]
