Structure:

Begin in this folder (prodScope). In this working directory, install the associated package (scopeUtils) using "pip install .".
This will give you the necessary dependencies and functions. Then proceed to... 

scopeProject: contains all the code for cleaning data, classifying 10Ks, generating analysis files. Run the Python files from the root folder (prodScope), like this: 'python scopeProject/0_constructsample/0_get_10k_text.py'. This will make the path references consistent with 'setup.py'. 

scopeProject/0_constructsample: 
- download_files: this folder contains SAS Studio files for downloading data from WRDS. Since WRDS terms of service does
not allow me to upload the raw data files, I provide my code containing the download specifications.
- 0_clean_cs_crsp.py: cleans and merges Compustat firm panel with CRSP returns. Requires Compustat Fundamentals Annuals, 
Compustat Company file, Compustat-CRSP link file from WRDS, and CRSP monthly returns. See code file for more details. 
- 1_cik_gvkey_link.py: merges Compustat file from 0_clean_cs_crsp.py-> WRDS CIK-gvkey link file -> WRDS 10K database.
**This file should be run on WRDS JupyterHub. It extracts files from WRDS Cloud.** 
- 2_get_10k_text.py: scrapes items 1, 1a, and 7 from SEC database and adds missing flags. Requires API key; enter it in the file.
- 3_chunk_10k_text.py: splits the output file from 0_get_10k_text.py into 73 chunks to process in parallel (see embed_items below).

(Below is still in progress. Code and analysis data will be uploaded soon.)
- embed_items: the files in this directory take the output from 3_chunk_10k_text.py and use the AnglE LLM to embed them into vectors.
Run these files on a High Performance Cluster to parallelize the necessary operations. The whole process took a few days on the 
Princeton HPC. I don't recommend running these files without adjusting them to your specific cluster configuration. 
    - emb_items.slurm -> 1_embed_items.py

scopeProject/1_classifyfirms: 

scopeProject/2_formvariables:

scopeProject/3_analysis:
 - summary stats
 - regressions
 - plots