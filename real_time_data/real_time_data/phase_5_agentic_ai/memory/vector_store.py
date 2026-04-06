"""
Vector Store - Persistent vector storage for semantic search and memory.
Uses FAISS for efficient similarity search with embedding management.
"""

import json
import logging
import pickle
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import asyncio
import numpy as np
from pathlib import Path

# Try to import vector store libraries
try:
    import faiss
    HAS_FAISS = True
except ImportError:
    HAS_FAISS = False
    logging.warning("faiss not installed. Using simple in-memory storage.")

try:
    from sentence_transformers import SentenceTransformer
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False
    logging.warning("sentence-transformers not installed. Using simple embeddings.")

from memory.embeddings import EmbeddingManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class VectorDocument:
    """Vector document with metadata."""
    id: str
    text: str
    embedding: np.ndarray
    metadata: Dict[str, Any]
    timestamp: datetime
    embedding_model: str
    embedding_dimension: int


@dataclass
class SearchResult:
    """Search result from vector store."""
    document: VectorDocument
    similarity: float
    rank: int


class VectorStore:
    """
    Vector store for semantic search and memory storage.
    Supports FAISS for efficient similarity search.
    """
    
    def __init__(
        self,
        store_path: Optional[str] = None,
        embedding_manager: Optional[EmbeddingManager] = None,
        dimension: int = 384,  # all-MiniLM-L6-v2 dimension
        index_type: str = "FlatIP",  # "FlatIP", "FlatL2", "IVFFlat", "HNSW"
        max_documents: int = 100000
    ):
        """
        Initialize Vector Store.
        
        Args:
            store_path: Path to persist vector store
            embedding_manager: Embedding manager instance
            dimension: Embedding dimension
            index_type: FAISS index type
            max_documents: Maximum number of documents to store
        """
        self.store_path = Path(store_path) if store_path else None
        self.dimension = dimension
        self.max_documents = max_documents
        
        # Initialize embedding manager
        self.embedding_manager = embedding_manager or EmbeddingManager()
        
        # Initialize FAISS index if available
        self.index = None
        self.documents = []
        self.document_map = {}
        
        if HAS_FAISS:
            self._initialize_faiss_index(index_type)
        else:
            logger.warning("FAISS not available. Using simple vector storage.")
        
        # Load existing store if path exists
        if self.store_path and self.store_path.exists():
            self.load()
        else:
            logger.info(f"Initialized new vector store with dimension {dimension}")
    
    def _initialize_faiss_index(self, index_type: str):
        """Initialize FAISS index based on type."""
        try:
            if index_type == "FlatIP":
                self.index = faiss.IndexFlatIP(self.dimension)
            elif index_type == "FlatL2":
                self.index = faiss.IndexFlatL2(self.dimension)
            elif index_type == "IVFFlat":
                quantizer = faiss.IndexFlatL2(self.dimension)
                self.index = faiss.IndexIVFFlat(quantizer, self.dimension, 100)
                self.index.train(np.random.rand(100, self.dimension).astype('float32'))
            elif index_type == "HNSW":
                self.index = faiss.IndexHNSWFlat(self.dimension, 32)
            else:
                self.index = faiss.IndexFlatIP(self.dimension)
            
            logger.info(f"Initialized FAISS index with type: {index_type}")
            
        except Exception as e:
            logger.error(f"Failed to initialize FAISS index: {str(e)}")
            self.index = None
    
    async def store_document(
        self,
        text: str,
        metadata: Dict[str, Any],
        embedding: Optional[np.ndarray] = None,
        document_id: Optional[str] = None
    ) -> str:
        """
        Store a document in the vector store.
        
        Args:
            text: Document text
            metadata: Document metadata
            embedding: Pre-computed embedding (optional)
            document_id: Custom document ID (optional)
            
        Returns:
            Document ID
        """
        try:
            # Generate document ID if not provided
            if document_id is None:
                doc_hash = hash(text + json.dumps(metadata, sort_keys=True))
                document_id = f"doc_{abs(doc_hash) % 1000000:06d}"
            
            # Generate embedding if not provided
            if embedding is None:
                embedding = await self.embedding_manager.get_embedding(text)
            
            # Ensure embedding is the right dimension
            if len(embedding) != self.dimension:
                logger.warning(f"Embedding dimension mismatch: {len(embedding)} != {self.dimension}")
                # Truncate or pad embedding
                if len(embedding) > self.dimension:
                    embedding = embedding[:self.dimension]
                else:
                    embedding = np.pad(embedding, (0, self.dimension - len(embedding)))
            
            # Create document
            document = VectorDocument(
                id=document_id,
                text=text,
                embedding=embedding,
                metadata=metadata,
                timestamp=datetime.now(),
                embedding_model=self.embedding_manager.model_name,
                embedding_dimension=self.dimension
            )
            
            # Add to storage
            self.documents.append(document)
            self.document_map[document_id] = document
            
            # Add to FAISS index if available
            if self.index is not None:
                # Reshape embedding for FAISS
                embedding_array = np.array([embedding], dtype='float32')
                self.index.add(embedding_array)
            
            # Limit storage size
            if len(self.documents) > self.max_documents:
                self._remove_oldest_documents(len(self.documents) - self.max_documents)
            
            logger.debug(f"Stored document: {document_id}")
            return document_id
            
        except Exception as e:
            logger.error(f"Error storing document: {str(e)}")
            raise
    
    async def search_documents(
        self,
        query: str,
        limit: int = 10,
        similarity_threshold: float = 0.5,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[SearchResult]:
        """
        Search documents by semantic similarity.
        
        Args:
            query: Search query
            limit: Maximum number of results
            similarity_threshold: Minimum similarity score
            filters: Metadata filters
            
        Returns:
            List of search results
        """
        try:
            # Get query embedding
            query_embedding = await self.embedding_manager.get_embedding(query)
            query_embedding = np.array([query_embedding], dtype='float32')
            
            if self.index is not None and HAS_FAISS:
                # Use FAISS for efficient search
                k = min(limit * 2, len(self.documents))  # Get extra for filtering
                
                if isinstance(self.index, faiss.IndexFlatIP):
                    # For Inner Product index, we need to normalize
                    faiss.normalize_L2(query_embedding)
                
                distances, indices = self.index.search(query_embedding, k)
                
                # Convert to results
                results = []
                for i, (distance, idx) in enumerate(zip(distances[0], indices[0])):
                    if idx < 0 or idx >= len(self.documents):
                        continue
                    
                    document = self.documents[idx]
                    
                    # Convert distance to similarity
                    similarity = self._distance_to_similarity(distance)
                    
                    # Apply threshold
                    if similarity < similarity_threshold:
                        continue
                    
                    # Apply filters
                    if filters and not self._matches_filters(document.metadata, filters):
                        continue
                    
                    results.append(SearchResult(
                        document=document,
                        similarity=similarity,
                        rank=i + 1
                    ))
                    
                    if len(results) >= limit:
                        break
                
            else:
                # Fallback: brute force search
                query_embedding = query_embedding[0]
                similarities = []
                
                for doc in self.documents:
                    similarity = self._calculate_cosine_similarity(query_embedding, doc.embedding)
                    
                    if similarity >= similarity_threshold:
                        if not filters or self._matches_filters(doc.metadata, filters):
                            similarities.append((similarity, doc))
                
                # Sort by similarity
                similarities.sort(key=lambda x: x[0], reverse=True)
                
                results = []
                for i, (similarity, doc) in enumerate(similarities[:limit]):
                    results.append(SearchResult(
                        document=doc,
                        similarity=similarity,
                        rank=i + 1
                    ))
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching documents: {str(e)}")
            return []
    
    def _distance_to_similarity(self, distance: float) -> float:
        """Convert FAISS distance to similarity score."""
        # For Inner Product: similarity = distance (since vectors are normalized)
        # For L2: similarity = 1 / (1 + distance)
        if isinstance(self.index, faiss.IndexFlatIP):
            return float(distance)
        else:
            return 1.0 / (1.0 + float(distance))
    
    def _calculate_cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """Calculate cosine similarity between two vectors."""
        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return float(dot_product / (norm1 * norm2))
    
    def _matches_filters(self, metadata: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """Check if metadata matches all filters."""
        for key, value in filters.items():
            if key not in metadata:
                return False
            
            if isinstance(value, list):
                if metadata[key] not in value:
                    return False
            elif metadata[key] != value:
                return False
        
        return True
    
    def _remove_oldest_documents(self, count: int):
        """Remove oldest documents from storage."""
        if count <= 0:
            return
        
        # Sort documents by timestamp
        self.documents.sort(key=lambda x: x.timestamp)
        
        # Remove oldest
        for _ in range(min(count, len(self.documents))):
            doc = self.documents.pop(0)
            del self.document_map[doc.id]
        
        # Rebuild FAISS index if needed
        if self.index is not None and HAS_FAISS:
            self._rebuild_faiss_index()
        
        logger.info(f"Removed {count} oldest documents")
    
    def _rebuild_faiss_index(self):
        """Rebuild FAISS index from current documents."""
        if not HAS_FAISS or not self.documents:
            return
        
        try:
            # Create new index
            if isinstance(self.index, faiss.IndexFlatIP):
                self.index = faiss.IndexFlatIP(self.dimension)
            elif isinstance(self.index, faiss.IndexFlatL2):
                self.index = faiss.IndexFlatL2(self.dimension)
            
            # Add all embeddings
            embeddings = np.array([doc.embedding for doc in self.documents], dtype='float32')
            self.index.add(embeddings)
            
        except Exception as e:
            logger.error(f"Error rebuilding FAISS index: {str(e)}")
            self.index = None
    
    async def update_document(
        self,
        document_id: str,
        text: Optional[str] = None,
        metadata: Optional[Dict] = None,
        embedding: Optional[np.ndarray] = None
    ) -> bool:
        """
        Update an existing document.
        
        Args:
            document_id: Document ID to update
            text: New text (optional)
            metadata: New metadata (optional)
            embedding: New embedding (optional)
            
        Returns:
            True if successful, False otherwise
        """
        if document_id not in self.document_map:
            return False
        
        try:
            document = self.document_map[document_id]
            
            # Update fields
            if text is not None:
                document.text = text
            
            if metadata is not None:
                document.metadata.update(metadata)
            
            if embedding is not None:
                document.embedding = embedding
            elif text is not None:
                # Re-embed if text changed
                document.embedding = await self.embedding_manager.get_embedding(text)
            
            document.timestamp = datetime.now()
            
            # Rebuild index if needed
            if embedding is not None or text is not None:
                self._rebuild_faiss_index()
            
            logger.debug(f"Updated document: {document_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating document: {str(e)}")
            return False
    
    async def delete_document(self, document_id: str) -> bool:
        """
        Delete a document from the vector store.
        
        Args:
            document_id: Document ID to delete
            
        Returns:
            True if successful, False otherwise
        """
        if document_id not in self.document_map:
            return False
        
        try:
            # Remove from lists
            document = self.document_map[document_id]
            self.documents = [d for d in self.documents if d.id != document_id]
            del self.document_map[document_id]
            
            # Rebuild index
            self._rebuild_faiss_index()
            
            logger.debug(f"Deleted document: {document_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting document: {str(e)}")
            return False
    
    def get_document(self, document_id: str) -> Optional[VectorDocument]:
        """
        Get a document by ID.
        
        Args:
            document_id: Document ID
            
        Returns:
            Document if found, None otherwise
        """
        return self.document_map.get(document_id)
    
    def get_documents_by_filter(
        self,
        filters: Dict[str, Any],
        limit: int = 100
    ) -> List[VectorDocument]:
        """
        Get documents by metadata filters.
        
        Args:
            filters: Metadata filters
            limit: Maximum number of documents
            
        Returns:
            List of matching documents
        """
        results = []
        
        for document in self.documents:
            if self._matches_filters(document.metadata, filters):
                results.append(document)
                
                if len(results) >= limit:
                    break
        
        return results
    
    async def batch_store_documents(
        self,
        documents: List[Tuple[str, Dict[str, Any]]],
        batch_size: int = 100
    ) -> List[str]:
        """
        Store multiple documents in batch.
        
        Args:
            documents: List of (text, metadata) tuples
            batch_size: Batch size for processing
            
        Returns:
            List of document IDs
        """
        document_ids = []
        
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            batch_tasks = []
            
            for text, metadata in batch:
                task = self.store_document(text, metadata)
                batch_tasks.append(task)
            
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Error in batch store: {str(result)}")
                else:
                    document_ids.append(result)
        
        return document_ids
    
    def save(self, path: Optional[str] = None):
        """
        Save vector store to disk.
        
        Args:
            path: Optional custom path
        """
        save_path = Path(path) if path else self.store_path
        
        if not save_path:
            logger.warning("No save path specified for vector store")
            return
        
        try:
            # Create directory if it doesn't exist
            save_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Prepare data for serialization
            save_data = {
                'documents': [
                    {
                        'id': doc.id,
                        'text': doc.text,
                        'embedding': doc.embedding.tolist() if isinstance(doc.embedding, np.ndarray) else doc.embedding,
                        'metadata': doc.metadata,
                        'timestamp': doc.timestamp.isoformat(),
                        'embedding_model': doc.embedding_model,
                        'embedding_dimension': doc.embedding_dimension
                    }
                    for doc in self.documents
                ],
                'dimension': self.dimension,
                'max_documents': self.max_documents,
                'saved_at': datetime.now().isoformat()
            }
            
            # Save to file
            with open(save_path, 'wb') as f:
                pickle.dump(save_data, f)
            
            # Save FAISS index separately if it exists
            if self.index is not None and HAS_FAISS:
                index_path = save_path.with_suffix('.faiss')
                faiss.write_index(self.index, str(index_path))
            
            logger.info(f"Saved vector store to {save_path} with {len(self.documents)} documents")
            
        except Exception as e:
            logger.error(f"Error saving vector store: {str(e)}")
    
    def load(self, path: Optional[str] = None):
        """
        Load vector store from disk.
        
        Args:
            path: Optional custom path
        """
        load_path = Path(path) if path else self.store_path
        
        if not load_path or not load_path.exists():
            logger.warning(f"Vector store file not found: {load_path}")
            return
        
        try:
            # Load data
            with open(load_path, 'rb') as f:
                save_data = pickle.load(f)
            
            # Restore documents
            self.documents = []
            self.document_map = {}
            
            for doc_data in save_data.get('documents', []):
                document = VectorDocument(
                    id=doc_data['id'],
                    text=doc_data['text'],
                    embedding=np.array(doc_data['embedding']),
                    metadata=doc_data['metadata'],
                    timestamp=datetime.fromisoformat(doc_data['timestamp']),
                    embedding_model=doc_data['embedding_model'],
                    embedding_dimension=doc_data['embedding_dimension']
                )
                
                self.documents.append(document)
                self.document_map[document.id] = document
            
            # Restore configuration
            self.dimension = save_data.get('dimension', self.dimension)
            self.max_documents = save_data.get('max_documents', self.max_documents)
            
            # Load FAISS index if it exists
            index_path = load_path.with_suffix('.faiss')
            if index_path.exists() and HAS_FAISS:
                try:
                    self.index = faiss.read_index(str(index_path))
                    logger.info(f"Loaded FAISS index from {index_path}")
                except Exception as e:
                    logger.error(f"Error loading FAISS index: {str(e)}")
                    self.index = None
            
            logger.info(f"Loaded vector store from {load_path} with {len(self.documents)} documents")
            
        except Exception as e:
            logger.error(f"Error loading vector store: {str(e)}")
            # Initialize fresh store on error
            self.documents = []
            self.document_map = {}
            self._initialize_faiss_index("FlatIP")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get vector store statistics."""
        # Calculate metadata distribution
        metadata_counts = {}
        for doc in self.documents:
            for key, value in doc.metadata.items():
                key_str = str(key)
                if key_str not in metadata_counts:
                    metadata_counts[key_str] = {}
                
                value_str = str(value)
                metadata_counts[key_str][value_str] = metadata_counts[key_str].get(value_str, 0) + 1
        
        # Calculate age distribution
        now = datetime.now()
        age_distribution = {
            'last_hour': 0,
            'last_day': 0,
            'last_week': 0,
            'last_month': 0,
            'older': 0
        }
        
        for doc in self.documents:
            age = now - doc.timestamp
            
            if age < timedelta(hours=1):
                age_distribution['last_hour'] += 1
            elif age < timedelta(days=1):
                age_distribution['last_day'] += 1
            elif age < timedelta(weeks=1):
                age_distribution['last_week'] += 1
            elif age < timedelta(days=30):
                age_distribution['last_month'] += 1
            else:
                age_distribution['older'] += 1
        
        return {
            'total_documents': len(self.documents),
            'embedding_dimension': self.dimension,
            'max_documents': self.max_documents,
            'storage_used_percent': (len(self.documents) / self.max_documents * 100) if self.max_documents > 0 else 0,
            'has_faiss_index': self.index is not None,
            'embedding_model': self.embedding_manager.model_name,
            'age_distribution': age_distribution,
            'metadata_fields': list(metadata_counts.keys()),
            'top_metadata_values': {
                k: dict(sorted(v.items(), key=lambda x: x[1], reverse=True)[:5])
                for k, v in list(metadata_counts.items())[:10]
            }
        }
    
    def clear(self, confirm: bool = False) -> bool:
        """
        Clear all documents from the vector store.
        
        Args:
            confirm: Confirmation flag
            
        Returns:
            True if cleared, False otherwise
        """
        if not confirm:
            logger.warning("Clear operation requires confirmation")
            return False
        
        try:
            self.documents = []
            self.document_map = {}
            
            if self.index is not None and HAS_FAISS:
                self._initialize_faiss_index("FlatIP")
            
            logger.info("Cleared all documents from vector store")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing vector store: {str(e)}")
            return False

if __name__ == "__main__":
    async def main():
        try:
            # Initialize embedding manager
            embedding_manager = EmbeddingManager()
            print("Embeddings created")
            
            # Initialize vector store
            vector_store = VectorStore(embedding_manager=embedding_manager)
            print("Vector store initialized")
            
        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            import traceback
            traceback.print_exc()

    asyncio.run(main())