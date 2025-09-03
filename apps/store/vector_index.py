import faiss, numpy as np
from sentence_transformers import SentenceTransformer


model = SentenceTransformer("all-MiniLM-L6-v2")
index = faiss.IndexFlatIP(384)


# chunker yields (chunk_id, text)
embs, ids = [], []
for cid, text in chunk_documents():
    vec = model.encode([text], normalize_embeddings=True)[0]
    embs.append(vec); ids.append(cid)
    index.add(np.array(embs))
    # persist ids mapping to disk