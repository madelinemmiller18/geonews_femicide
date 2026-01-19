#!/bin/bash

#SBATCH --job-name=10hr_geolocatedf32DataLiteracy_250gb
# give it any name you want

#SBATCH --cpus-per-task=1
# max 24 per node

#SBATCH --partition=day
# choose out of day, week, month depending on job duration

#SBATCH --mem-per-cpu=250GB
# max 251GB per node

#SBATCH --gres=gpu:0
# how many gpus to use
# each node has 4 gpus

#SBATCH --time=10:00:00
# job length: the job will run either until completion or until this timer runs out
# "minutes:seconds", "hours:minutes:seconds", "days-hours", "days-hours:minutes" and "days-hours:minutes:seconds"

#SBATCH --error=job.%J.err
# %J is the job ID, errors will be written to this file
# write the error output to job.*jobID*.err

#SBATCH --output=job.%J.out
# the output will be written in this file
# write the standard output to job.*jobID*.out


#SBATCH --mail-type=ALL
# write a mail if a job begins, ends, fails, gets requeued or stages out
# options: NONE, BEGIN, END, FAIL, REQUEUE, ALL

#SBATCH --mail-user=madeline-marie.miller@student.uni-tuebingen.de
# your email

# here will be your commands for running the script

#copy all needed data to the jobs scratch folder
echo "copy data"
cp -R /home/miller/data_pull/CommonCrawlNews.db /scratch/$SLURM_JOB_ID/
echo "finished copying common crawl data"

cp -R /home/miller/data_pull/NewsIndex_f32_new.usearch /scratch/$SLURM_JOB_ID/
echo "finished copying usearch data"
echo $SLURM_JOB_ID

####
#c) Execute your tensorflow code in a specific singularity container
#d) Write your checkpoints to your home directory, so that you still have them if your job fails
#cnn_minst.py <model save path> <mnist data path>
####

#matches_500000_F32_femicide.py \

for script in \
    matches_150000_F32_femicide.py \
    matches_1000000_F32_femicide.py
do
    echo "Running $script"
    singularity exec --nv --cleanenv --env SLURM_JOB_ID=$SLURM_JOB_ID \
        /home/miller/singularity_build_m/geolocatednews_np1_tfkeras_image.simg \
        python3 /home/miller/matches_scripts/$script || echo "$script failed but continuing"
done
