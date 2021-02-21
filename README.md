# Big data with apache spark in scala
we use the IDE Intellij

# Install java for scala
set the environment of java

cat <<EOF | sudo tee /etc/profile.d/jdk14.sh
export JAVA_HOME=/usr/lib/jvm/jdk-14.0.1
export PATH=\$PATH:\$JAVA_HOME/bin
EOF
	
source /etc/profile.d/jdk14.sh
java -version

# script count-words.scala
we count the number of words of Don Quijote de la Mancha by Cervantes
separatd by a space and sorted by word

download of the file  => https://www.gutenberg.org/files/5921/5921-0.txt
.txt file


