import os
import numpy as np
import pandas as pd
import sqlite3
from usearch.index import Index
from sentence_transformers import SentenceTransformer
print("libraries loaded")

#set query and title
numberMatches = 500000
query_text = "Tötungsdelikt mit weiblichem Opfer"
query_name = "32_homicide-female_DE"
file_name = f"{numberMatches}_{query_name}"
export_path = f"/home/miller/{file_name}.csv"

cwd = os.getcwd()
print(f"working directory: {cwd}")

print("libraries loaded")
job_id = os.environ["SLURM_JOB_ID"]

# Database path
DB_PATH = f'/scratch/{job_id}/CommonCrawlNews.db'

# Create database indexes for faster queries
print("Setting up database indexes...")
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create indexes if they don't exist
cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_article_vectors_hashed_id 
ON Article_Vectors(hashed_id);
""")

cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_article_vectors_article_id 
ON Article_Vectors(article_id);
""")

cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_article_locations_article_id 
ON Article_Locations(article_id);
""")

cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_article_locations_location_id 
ON Article_Locations(location_id);
""")

# Optimize SQLite for better performance
cursor.execute("PRAGMA temp_store = MEMORY")
cursor.execute("PRAGMA cache_size = -2000000")  # 2GB cache
cursor.execute("PRAGMA journal_mode = WAL")  # Write-Ahead Logging for better concurrency

conn.commit()
print("Database indexes created and optimized")

# Load the vector search index
print("starting usearch")
index = Index(ndim=1024, metric="cos", dtype="f32")
index.load(f"/scratch/{job_id}/NewsIndex_f32_new.usearch")
print("loaded usearch")

# Load the SentenceTransformer model
print("loading sentence transformer model")
model = SentenceTransformer("mixedbread-ai/deepset-mxbai-embed-de-large-v1")
print("loaded sentence transformer model")

# Encode the query text with normalization
query_embedding = model.encode(f"query: {query_text}", normalize_embeddings=True)

# Perform search 
print("performing semantic search")
matches = index.search(query_embedding, numberMatches, exact=True)
print(f"{len(matches)} matches retrieved")

# Extract distances and IDs using numpy for efficiency
distances = np.array([match.distance for match in matches])
ids_f32 = [str(match.key) for match in matches]
print("semantic search done")

# Process results in batches
if ids_f32:
    BATCH_SIZE = 30000
    all_results = []
    
    print(f"Processing {len(ids_f32)} IDs in batches of {BATCH_SIZE}")
    total_batches = (len(ids_f32) - 1) // BATCH_SIZE + 1
    
    for i in range(0, len(ids_f32), BATCH_SIZE):
        batch_ids = ids_f32[i:i + BATCH_SIZE]
        batch_distances = distances[i:i + BATCH_SIZE]
        
        # Prepare SQL query for this batch
        placeholders = ', '.join('?' for _ in batch_ids)
        query = f"""
        SELECT a.id, a.url, a.hostname, a.date, av.hashed_id, a.date_crawled, 
               l.loc_normal, l.latitude, l.longitude, l.NUTS, '{query_text}' AS query_string, '{query_name}' AS query_name
        FROM Article_Vectors av
        INNER JOIN Articles a ON a.id = av.article_id
        INNER JOIN Article_Locations al ON a.id = al.article_id
        INNER JOIN Locations l ON al.location_id = l.location_id
        WHERE av.hashed_id IN ({placeholders});
        """
        
        batch_num = i // BATCH_SIZE + 1
        print(f"Processing batch {batch_num}/{total_batches}")
        
        # Execute the query for this batch
        batch_result = pd.read_sql_query(query, conn, params=batch_ids)
        
        # Add cosine distances for this batch
        dist_map = dict(zip(batch_ids, batch_distances))
        batch_result["cos_dist"] = batch_result["hashed_id"].map(dist_map)
        all_results.append(batch_result)
        
        print(f"Batch {batch_num} complete: {len(batch_result)} results")
    
    # Combine all batches into a single DataFrame
    db_result = pd.concat(all_results, ignore_index=True)
    print(f"Total results retrieved: {len(db_result)}")
else:
    db_result = pd.DataFrame()
    print("No matches found")

# Close the database connection
conn.close()

# Display sample results
print("\nSample results:")
print(db_result.head())

# Save to CSV
db_result.to_csv(export_path, index=False)
print(f"\nResults saved to {export_path}")
print(f"Total rows saved: {len(db_result)}")