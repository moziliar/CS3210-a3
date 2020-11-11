#!/bin/bash
# expects preset hostfile as $1

if [[ $1 == "xeons" ]]; then
  # xeon
  NP=("20" "40" "60" "80")
elif [[ $1 == "xeons-i7s" ]]; then 
  # xeon and i7
  NP=("8" "16" "24" "32" "46" "56")
elif [[ $1 == "i7s" ]]; then
  # i7
  NP=("8" "16" "24" "32")
# else
  # compute locally
  # NP=("4","8")
fi

$(make compile-par)

# can't test v1, v3

# bins=("distr-sched", "distr-sched-v1", "distr-sched-v2", "distr-sched-v3", "distr-sched-v4")
bins=("distr-sched" "distr-sched-v2" "distr-sched-v4")
cases=("chains" "heaps" "sparse" "dense" "fan" "extra-chain" "almost-chain")
# cases=("8 1 1 0.00 < chains.in", "2 2 2 0.00 < heaps.in", "12 0 10 0.16 < sparse.in", "5 3 5 0.50 < dense.in")

# Different configurations and numbers of each type of lab machine
# Different input task graph parameters
# Different sets of initial seed tasks (including the provided set of input files)


# $1 - command to run as a string
min_of_three_runs() {
    # run 3 times
    # extract time value and compare with min
    # choose the output for the smaller one
    # arbitary large number, can't possibly take more than 60000 seconds right?
    local min="1000000"
    local min_output=""
    for t in {1..3}; do
        local output=$($1 | tail -n +2)
        local time_taken=$(get_time "$output")
        local is_less_than=$(echo "${time_taken} < $min" | bc -l)
        if [[ $is_less_than == "1" ]]; then
            min_output="$output"
        fi
    done
    echo "$min_output"
}

# All the procs should end 
# at roughly the same time so we just take the first one
# and check its time
get_time() {
    local temp=$(echo "$1" | head -n 1 | awk '{print substr($5, 1, length($5)-2)}')
    echo ${temp//,}
}


# parse execution output to determine runtime
# take the min runtime's results and write to file
# for each rank, 
# space separated for each rank
# print util total #1 #2 #3 #4 #5
echo ""
for case in "${cases[@]}"; do
  for bin in "${bins[@]}"; do
    for n in "${NP[@]}"; do
    echo "$case - $bin - $n"
    to_run="make config-s-${case} NP=${n} HF=$1 BIN=${bin}"

    output=$(min_of_three_runs "$to_run")
    echo "compute-time execution-time #1 #2 #3 #4 #5"
    echo "$output" | awk '{print substr($3,1,length($3)-2), substr($5,1,length($5)-2), $9, $10, $11, $12, $13}'
    echo ""
    done
  done
done
