"""
Embedding Manager - Manages text embeddings for vector storage.
Supports multiple embedding models with caching and batching.
"""

import json
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import numpy as np
from functools import lru_cache

# Try to import embedding libraries
try:
    from sentence_transformers import SentenceTransformer
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False
    logging.warning("sentence-transformers not installed. Using simple embeddings.")

try:
    import openai
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False
    logging.warning("openai not installed. OpenAI embeddings unavailable.")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EmbeddingModel(Enum):
    """Supported embedding models."""
    # Local models
    MINI_LM = "all-MiniLM-L6-v2"
    MPNET = "all-mpnet-base-v2"
    DISTILROBERTA = "all-distilroberta-v1"
    
    # OpenAI models
    OPENAI_ADA = "text-embedding-ada-002"
    OPENAI_LARGE = "text-embedding-3-large"
    
    # Custom models
    CUSTOM = "custom"


class EmbeddingManager:
    """
    Manages text embeddings with model selection, caching, and batching.
    """
    
    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        cache_size: int = 10000,
        batch_size: int = 32,
        max_length: int = 512,
        device: str = "cpu"
    ):
        """
        Initialize Embedding Manager.
        
        Args:
            model_name: Name of the embedding model
            cache_size: Maximum number of embeddings to cache
            batch_size: Batch size for embedding generation
            max_length: Maximum text length for embedding
            device: Device to run model on ("cpu" or "cuda")
        """
        self.model_name = model_name
        self.cache_size = cache_size
        self.batch_size = batch_size
        self.max_length = max_length
        self.device = device
        
        # Initialize cache
        self.embedding_cache = {}
        self.cache_timestamps = {}
        self.cache_hits = 0
        self.cache_misses = 0
        
        # Initialize model
        self.model = None
        self.model_dimension = None
        
        self._initialize_model()
        
        logger.info(f"Embedding Manager initialized with model: {model_name}")
    
    def _initialize_model(self):
        """Initialize the embedding model."""
        try:
            if self.model_name.startswith("text-embedding"):
                # OpenAI model
                if not HAS_OPENAI:
                    raise ImportError("OpenAI not installed for OpenAI embeddings")
                
                self.model_type = "openai"
                self.model_dimension = self._get_openai_dimension(self.model_name)
                logger.info(f"Using OpenAI embedding model: {self.model_name}")
                
            else:
                # Local SentenceTransformer model
                if not HAS_SENTENCE_TRANSFORMERS:
                    raise ImportError("sentence-transformers not installed for local embeddings")
                
                self.model_type = "local"
                self.model = SentenceTransformer(self.model_name, device=self.device)
                self.model_dimension = self.model.get_sentence_embedding_dimension()
                logger.info(f"Loaded local embedding model: {self.model_name} (dimension: {self.model_dimension})")
                
        except Exception as e:
            logger.error(f"Failed to initialize embedding model: {str(e)}")
            
            # Fallback to simple embeddings
            self.model_type = "simple"
            self.model_dimension = 384  # Match MiniLM dimension
            logger.warning(f"Using simple embedding fallback (dimension: {self.model_dimension})")
    
    def _get_openai_dimension(self, model_name: str) -> int:
        """Get embedding dimension for OpenAI models."""
        dimensions = {
            "text-embedding-ada-002": 1536,
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072
        }
        return dimensions.get(model_name, 1536)
    
    async def get_embedding(
        self,
        text: str,
        use_cache: bool = True,
        normalize: bool = True
    ) -> np.ndarray:
        """
        Get embedding for a single text.
        
        Args:
            text: Input text
            use_cache: Whether to use caching
            normalize: Whether to normalize the embedding
            
        Returns:
            Embedding vector
        """
        # Check cache
        if use_cache:
            cache_key = hash(text) % 1000000
            if cache_key in self.embedding_cache:
                self.cache_hits += 1
                self.cache_timestamps[cache_key] = datetime.now()
                return self.embedding_cache[cache_key].copy()
        
        self.cache_misses += 1
        
        # Generate embedding
        if self.model_type == "openai":
            embedding = await self._get_openai_embedding(text)
        elif self.model_type == "local":
            embedding = await self._get_local_embedding(text)
        else:
            embedding = self._get_simple_embedding(text)
        
        # Normalize if requested
        if normalize and embedding is not None:
            norm = np.linalg.norm(embedding)
            if norm > 0:
                embedding = embedding / norm
        
        # Cache the result
        if use_cache and embedding is not None:
            self._add_to_cache(cache_key, embedding)
        
        return embedding
    
    async def get_embeddings_batch(
        self,
        texts: List[str],
        use_cache: bool = True,
        normalize: bool = True
    ) -> List[np.ndarray]:
        """
        Get embeddings for a batch of texts.
        
        Args:
            texts: List of input texts
            use_cache: Whether to use caching
            normalize: Whether to normalize embeddings
            
        Returns:
            List of embedding vectors
        """
        embeddings = []
        uncached_texts = []
        uncached_indices = []
        
        # Check cache for each text
        for i, text in enumerate(texts):
            if use_cache:
                cache_key = hash(text) % 1000000
                if cache_key in self.embedding_cache:
                    self.cache_hits += 1
                    self.cache_timestamps[cache_key] = datetime.now()
                    embeddings.append(self.embedding_cache[cache_key].copy())
                    continue
            
            uncached_texts.append(text)
            uncached_indices.append(i)
            embeddings.append(None)  # Placeholder
        
        # Generate embeddings for uncached texts
        if uncached_texts:
            self.cache_misses += len(uncached_texts)
            
            if self.model_type == "openai":
                batch_embeddings = await self._get_openai_embeddings_batch(uncached_texts)
            elif self.model_type == "local":
                batch_embeddings = await self._get_local_embeddings_batch(uncached_texts)
            else:
                batch_embeddings = [self._get_simple_embedding(t) for t in uncached_texts]
            
            # Normalize and cache
            for idx, embedding, text in zip(uncached_indices, batch_embeddings, uncached_texts):
                if embedding is not None:
                    if normalize:
                        norm = np.linalg.norm(embedding)
                        if norm > 0:
                            embedding = embedding / norm
                    
                    if use_cache:
                        cache_key = hash(text) % 1000000
                        self._add_to_cache(cache_key, embedding)
                    
                    embeddings[idx] = embedding
        
        return embeddings
    
    async def _get_openai_embedding(self, text: str) -> np.ndarray:
        """Get embedding from OpenAI API."""
        try:
            # Truncate text if too long
            if len(text) > self.max_length * 4:  # Rough character estimate
                text = text[:self.max_length * 4]
            
            response = await asyncio.to_thread(
                openai.Embedding.create,
                model=self.model_name,
                input=text
            )
            
            embedding = np.array(response['data'][0]['embedding'], dtype=np.float32)
            return embedding
            
        except Exception as e:
            logger.error(f"OpenAI embedding error: {str(e)}")
            
            # Fallback to local model if available
            if self.model_type != "local" and HAS_SENTENCE_TRANSFORMERS:
                logger.info("Falling back to local embedding model")
                self.model_type = "local"
                self.model = SentenceTransformer("all-MiniLM-L6-v2", device=self.device)
                self.model_dimension = self.model.get_sentence_embedding_dimension()
                return await self._get_local_embedding(text)
            
            # Fallback to simple embedding
            return self._get_simple_embedding(text)
    
    async def _get_openai_embeddings_batch(self, texts: List[str]) -> List[np.ndarray]:
        """Get embeddings from OpenAI API in batch."""
        try:
            # Truncate texts if too long
            truncated_texts = []
            for text in texts:
                if len(text) > self.max_length * 4:
                    truncated_texts.append(text[:self.max_length * 4])
                else:
                    truncated_texts.append(text)
            
            response = await asyncio.to_thread(
                openai.Embedding.create,
                model=self.model_name,
                input=truncated_texts
            )
            
            embeddings = []
            for item in response['data']:
                embeddings.append(np.array(item['embedding'], dtype=np.float32))
            
            return embeddings
            
        except Exception as e:
            logger.error(f"OpenAI batch embedding error: {str(e)}")
            
            # Fallback to sequential processing
            embeddings = []
            for text in texts:
                embedding = await self._get_openai_embedding(text)
                embeddings.append(embedding)
            
            return embeddings
    
    async def _get_local_embedding(self, text: str) -> np.ndarray:
        """Get embedding from local model."""
        try:
            if self.model is None:
                raise ValueError("Local model not initialized")
            
            # Encode text
            embedding = await asyncio.to_thread(
                self.model.encode,
                text,
                convert_to_numpy=True,
                normalize_embeddings=False,
                show_progress_bar=False
            )
            
            return embedding.astype(np.float32)
            
        except Exception as e:
            logger.error(f"Local embedding error: {str(e)}")
            return self._get_simple_embedding(text)
    
    async def _get_local_embeddings_batch(self, texts: List[str]) -> List[np.ndarray]:
        """Get embeddings from local model in batch."""
        try:
            if self.model is None:
                raise ValueError("Local model not initialized")
            
            # Process in batches
            all_embeddings = []
            
            for i in range(0, len(texts), self.batch_size):
                batch_texts = texts[i:i + self.batch_size]
                
                batch_embeddings = await asyncio.to_thread(
                    self.model.encode,
                    batch_texts,
                    convert_to_numpy=True,
                    normalize_embeddings=False,
                    show_progress_bar=False,
                    batch_size=min(self.batch_size, len(batch_texts))
                )
                
                # Convert to list of numpy arrays
                if isinstance(batch_embeddings, np.ndarray):
                    for j in range(len(batch_texts)):
                        all_embeddings.append(batch_embeddings[j].astype(np.float32))
                else:
                    all_embeddings.extend([emb.astype(np.float32) for emb in batch_embeddings])
            
            return all_embeddings
            
        except Exception as e:
            logger.error(f"Local batch embedding error: {str(e)}")
            
            # Fallback to sequential processing
            embeddings = []
            for text in texts:
                embedding = await self._get_local_embedding(text)
                embeddings.append(embedding)
            
            return embeddings
    
    def _get_simple_embedding(self, text: str) -> np.ndarray:
        """
        Simple embedding fallback using TF-IDF like approach.
        This is a basic implementation for when proper models aren't available.
        """
        try:
            # Simple character n-gram based embedding
            text_lower = text.lower()
            n = 3  # Use 3-grams
            
            # Generate n-grams
            ngrams = []
            for i in range(len(text_lower) - n + 1):
                ngrams.append(text_lower[i:i + n])
            
            # Create simple hash-based embedding
            embedding = np.zeros(self.model_dimension, dtype=np.float32)
            
            for ngram in ngrams:
                # Simple hash to distribute across dimensions
                hash_val = hash(ngram) % self.model_dimension
                embedding[hash_val] += 1.0
            
            # Normalize
            norm = np.linalg.norm(embedding)
            if norm > 0:
                embedding = embedding / norm
            
            return embedding
            
        except Exception as e:
            logger.error(f"Simple embedding error: {str(e)}")
            # Return random embedding as last resort
            return np.random.randn(self.model_dimension).astype(np.float32)
    
    def _add_to_cache(self, cache_key: int, embedding: np.ndarray):
        """Add embedding to cache with LRU eviction."""
        # Add to cache
        self.embedding_cache[cache_key] = embedding.copy()
        self.cache_timestamps[cache_key] = datetime.now()
        
        # Evict if cache is full
        if len(self.embedding_cache) > self.cache_size:
            # Find oldest entry
            oldest_key = min(self.cache_timestamps.items(), key=lambda x: x[1])[0]
            del self.embedding_cache[oldest_key]
            del self.cache_timestamps[oldest_key]
    
    def clear_cache(self):
        """Clear the embedding cache."""
        self.embedding_cache.clear()
        self.cache_timestamps.clear()
        self.cache_hits = 0
        self.cache_misses = 0
        logger.info("Cleared embedding cache")
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get cache statistics."""
        hit_rate = self.cache_hits / (self.cache_hits + self.cache_misses) if (self.cache_hits + self.cache_misses) > 0 else 0
        
        # Calculate age distribution
        now = datetime.now()
        age_distribution = {
            'last_hour': 0,
            'last_day': 0,
            'last_week': 0,
            'older': 0
        }
        
        for timestamp in self.cache_timestamps.values():
            age = now - timestamp
            
            if age < timedelta(hours=1):
                age_distribution['last_hour'] += 1
            elif age < timedelta(days=1):
                age_distribution['last_day'] += 1
            elif age < timedelta(weeks=1):
                age_distribution['last_week'] += 1
            else:
                age_distribution['older'] += 1
        
        return {
            'cache_size': len(self.embedding_cache),
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'cache_hit_rate': f"{hit_rate:.2%}",
            'age_distribution': age_distribution,
            'max_cache_size': self.cache_size
        }
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the embedding model."""
        return {
            'model_name': self.model_name,
            'model_type': self.model_type,
            'dimension': self.model_dimension,
            'max_length': self.max_length,
            'batch_size': self.batch_size,
            'device': self.device,
            'available': self.model is not None or self.model_type == "openai"
        }
    
    async def change_model(
        self,
        model_name: str,
        device: Optional[str] = None
    ) -> bool:
        """
        Change the embedding model.
        
        Args:
            model_name: New model name
            device: Device to run model on
            
        Returns:
            True if successful, False otherwise
        """
        try:
            old_model = self.model
            old_model_name = self.model_name
            
            # Update parameters
            self.model_name = model_name
            if device:
                self.device = device
            
            # Clear cache
            self.clear_cache()
            
            # Reinitialize model
            self.model = None
            self._initialize_model()
            
            logger.info(f"Changed embedding model from {old_model_name} to {model_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error changing embedding model: {str(e)}")
            
            # Restore old model
            self.model_name = old_model_name
            self.model = old_model
            
            return False
    
    def get_embedding_dimension(self) -> int:
        """Get the embedding dimension."""
        return self.model_dimension or 384
    
    @staticmethod
    def list_available_models() -> Dict[str, List[str]]:
        """List available embedding models."""
        models = {
            'local': [],
            'openai': [],
            'other': []
        }
        
        # Local models (if sentence-transformers is available)
        if HAS_SENTENCE_TRANSFORMERS:
            models['local'].extend([
                "all-MiniLM-L6-v2",
                "all-mpnet-base-v2",
                "all-distilroberta-v1",
                "paraphrase-multilingual-MiniLM-L12-v2"
            ])
        
        # OpenAI models
        if HAS_OPENAI:
            models['openai'].extend([
                "text-embedding-ada-002",
                "text-embedding-3-small",
                "text-embedding-3-large"
            ])
        
        # Other models
        models['other'].extend([
            "custom",
            "simple"
        ])
        
        return models