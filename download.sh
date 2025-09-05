DATA_PATH=$1

python make_wget.py --yaml configs/official-0425/OLMo2-1B-stage1.yaml --data-dir $DATA_PATH --trim-prefix /preprocessed/ --include-prefix starcoder,olmo-mix,dclm,starcoder,pes2o,proof-pile-2 --out wget_bash.sh
