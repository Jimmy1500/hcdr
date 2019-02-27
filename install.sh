#!/usr/bin/env bash

function setup_aliases {
    cat <<EOF >> $HOME/.bashrc
# some more ls aliases
alias ll='ls -alF'
alias la='ls -A'
alias l='ls -CF'
EOF
}

function setup_spark {
    cat <<EOF >>$HOME/.bashrc
    
# set PATH so to include spark
if [ -d "\$HOME/bin/spark-2.4.0-bin-hadoop2.7/bin" ]; then
    PATH="\$HOME/bin/spark-2.4.0-bin-hadoop2.7/bin:\$PATH"
fi
EOF
}

set -x

Download & decompress spark
wget http://apache.spinellicreations.com/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz

tar -xzvf spark-2.4.0-bin-hadoop2.7.tgz
rm spark-2.4.0-bin-hadoop2.7.tgz

mkdir -p $HOME/bin
mv spark-2.4.0-bin-hadoop2.7 $HOME/bin

if [ -f $HOME/.bashrc ]; then
    BASHRC_SPARK=`gawk 'BEGIN{FS=" "; setup_spark=0;} { for (i = 1; i <= NF; ++i) { if ($i ~ "spark"){ setup_spark = 1; exit; } } } END{ if (setup_spark) {print "Y"} else { print "N"} }' < $HOME/.bashrc`
else
    touch $HOME/.bashrc
    BASHRC_SPARK="N"
fi

OS=`uname -s`
Architecture=`uname -m`

if [ $OS == "Darwin" ] || [[ $OS == "Mac"* ]]; then
    echo "OS/Architecture: ${OS}/${Architecture} detected, flushing $HOME/.bashrc with spark settings"
    brew install python
    if [ $BASHRC_SPARK == "N" ]; then
        setup_aliases
        setup_spark
    fi
elif [ $OS == "Linux" ]; then
    echo "OS/Architecture: ${OS}/${Architecture} detected, flusing $HOME/.bashrc with spark settings"
    if [ $BASHRC_SPARK == "N" ]; then
        setup_spark
    fi
fi

set +x
