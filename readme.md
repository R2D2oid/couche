
. activate.sh

module load python/3.11.5 scipy-stack/2024a StdEnv/2023 arrow/17.0.0
cd /home/zahrav/projects/def-jjclark/zahrav/repos/couche
source /home/zahrav/projects/def-jjclark/zahrav/repos/couche/ENV/bin/activate

python webui.py --share

