import subprocess
from pathlib import Path
EXEC = "go run src/BST.go"

def main():
    run(r"input\fine.txt", [100000, 1 ,2 ,4, 8 ,16], [1 ,2, 4, 8, 16])
    run(r"input\coarse.txt", [100, 1 ,2 ,4, 8 ,16], [1 ,2, 4, 8, 16])

def run(input, hash_workers, comp_workers):
  for hash in hash_workers:
    run_single(input, f"-hash-workers {hash}")
    run_single(input, f"-hash-workers {hash} -use-mutex")
    for comp in comp_workers:
      run_single(input, f"-hash-workers {hash} -comp-workers {comp}")
      run_single(input, f"-hash-workers {hash} -comp-workers {comp} -use-mutex")


def run_single(input, additonal_args):
    hashTime=0
    hashGroupTime=0
    compareTreeTime=0
    for x in range(0, 10):
      path_in = Path(input)
      cmd = f"{EXEC} {additonal_args}"
      command_to_run = f"{cmd} -input={path_in.absolute().as_posix()}"
      p = subprocess.check_output(command_to_run, shell=True).decode("ascii")

      for line in p.splitlines():
        if line.startswith("hashTime"):
          hashTime += float(line.replace("hashTime: ", ""))
        elif line.startswith("compareTreeTime"): # ignore the timing things.
          compareTreeTime += float(line.replace("compareTreeTime: ", ""))
        elif line.startswith("hashGroupTime"): # ignore the timing things.
          hashGroupTime += float(line.replace("hashGroupTime: ", ""))
    print(f"{input} hashTime {hashTime/10} {additonal_args}")
    print(f"{input} hashGroupTime {hashGroupTime/10} {additonal_args}")
    print(f"{input} compareTreeTime {compareTreeTime/10} {additonal_args}")

if __name__ == "__main__":
    main()