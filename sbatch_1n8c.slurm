#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=00:05:00
module load Python/3.6.4-intel-2017.u2
mpirun python Dejun_spartan_trial1.py
