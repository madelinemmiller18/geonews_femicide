print("python test is running")
import os
cwd = os.getcwd()
print("working directory:")
print(cwd)
#---
import pandas as pd
import sqlite3
from usearch.index import Index
from sentence_transformers import SentenceTransformer
print("libraries loaded")

job_id = os.environ["SLURM_JOB_ID"]
print("starting usearch")

# Load the vector search index with 1024-dimensional vectors using cosine similarity and 32-bit float precision
index = Index(ndim=1024, metric="cos", dtype="f32")
index.load(f"/scratch/{job_id}/NewsIndex_f32_new.usearch")
print("loaded usearch")

# Database connection setup
DB_PATH = f'/scratch/{job_id}/CommonCrawlNews.db'
conn = sqlite3.connect(DB_PATH)
print("loaded database")

# Load the SentenceTransformer model
model = SentenceTransformer("mixedbread-ai/deepset-mxbai-embed-de-large-v1")
print("loaded sentence transformer model")

# Encode the query text with "query: " prompt into an embedding with normalization for better retrieval
query_embedding = model.encode("query: femicide", normalize_embeddings=True)

# Perform a search in the index, retrieving up to 150000 matches
matches = index.search(query_embedding, 150000)
print(f"{len(matches)} matches retrieved")

# Extract distances and IDs from search results
distances = [match.distance for match in matches]
ids_f32 = [str(match.key) for match in matches]
print("semantic search done")

# Process results in batches to avoid SQL parameter limits
if ids_f32:
    BATCH_SIZE = 30000  # Process 30k IDs at a time (well under SQLite's limit)
    all_results = []
    
    print(f"Processing {len(ids_f32)} IDs in batches of {BATCH_SIZE}")
    
    for i in range(0, len(ids_f32), BATCH_SIZE):
        batch_ids = ids_f32[i:i + BATCH_SIZE]
        batch_distances = distances[i:i + BATCH_SIZE]
        
        # Prepare SQL query for this batch
        placeholders = ', '.join('?' for _ in batch_ids)
        query = f"""
        SELECT a.id, a.url, a.hostname, a.date, av.hashed_id, a.date_crawled, 
               l.loc_normal, l.latitude, l.longitude, l.NUTS
        FROM Articles a
        JOIN Article_Vectors av ON a.id = av.article_id
        JOIN Article_Locations al ON a.id = al.article_id
        JOIN Locations l ON al.location_id = l.location_id
        WHERE av.hashed_id IN ({placeholders});
        """
        
        print(f"Processing batch {i//BATCH_SIZE + 1}/{(len(ids_f32)-1)//BATCH_SIZE + 1}")
        
        # Execute the query for this batch
        batch_result = pd.read_sql_query(query, conn, params=batch_ids)
        
        # Add cosine distances for this batch
        dist_map = dict(zip(batch_ids, batch_distances))
        batch_result["cos_dist"] = batch_result["hashed_id"].map(dist_map)
        all_results.append(batch_result)

        db_results_progress = pd.concat(all_results, ignore_index=True)
        print(f"results retrieved so far: {len(db_results_progress)}")
        
    # Combine all batches into a single DataFrame
    db_result = pd.concat(all_results, ignore_index=True)
    print(f"Total results retrieved: {len(db_result)}")
else:
    db_result = pd.DataFrame()

# Close the database connection
conn.close()

# Display the results
print(db_result.head())

# Save to CSV
output_path = "/home/miller/matches_150000_F32_femicide.csv"
db_result.to_csv(output_path, index=False)
print(f"Results saved to {output_path}")