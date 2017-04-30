#!/usr/bin/env bash
GASPAR="iman"
SUBMISSION_DIR="submission/$GASPAR/exercise2"
SOURCE_BASE="src/main/java"

rm -f Task4.pdf Task4.log Task4.dvi Task4.aux
pdflatex Task4.tex

rm -rf $SUBMISSION_DIR
mkdir -p $SUBMISSION_DIR/task1
mkdir -p $SUBMISSION_DIR/task2
mkdir -p $SUBMISSION_DIR/task3
mkdir -p $SUBMISSION_DIR/task4

FILES_T1=($SOURCE_BASE/Skyline.java)
FILES_T2=($SOURCE_BASE/ColumnStore.java)
FILES_T3=($SOURCE_BASE/CompressedColumnStore.java)
FILES_T4=($SOURCE_BASE/Task4.java Task4.pdf)

copy_files() {
    par_name=$1[@]
    task_f=$2
    FILES=("${!par_name}")
    for file in "${FILES[@]}"; do
        cp $file $SUBMISSION_DIR/$task_f/.
        if [ $? -ne 0 ]
        then
            exit 1
        fi
    done;
}

copy_files FILES_T1 task1
copy_files FILES_T2 task2
copy_files FILES_T3 task3
copy_files FILES_T4 task4

zip -r submission.zip submission/$GASPAR/
