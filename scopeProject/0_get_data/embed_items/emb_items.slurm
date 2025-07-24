#!/bin/bash
#SBATCH --job-name=emb_items      # create a short name for your job
#SBATCH --nodes=1            # use 1 node
#SBATCH --ntasks=1           # run 1 task (we manage multiple processes manually)
#SBATCH --mem-per-cpu=75G ##40G    # memory per CPU
#SBATCH --cpus-per-task=2   
#SBATCH --gres=gpu:2        
#SBATCH --time=01:02:00      # set a time limit
#SBATCH --dependency=singleton
#SBATCH --array=0-100              # job array with index values 
#SBATCH --mail-type=begin        # send email when job begins
#SBATCH --mail-type=end          # send email when job ends
#SBATCH --mail-user=anshuc@princeton.edu
#SBATCH --output=/home/anshuc/slurm/slurm-%A.%a.out  # stdout file
module purge
module load anaconda3/2024.2
conda activate gpu_enabled

srun python 1_embed_items.py