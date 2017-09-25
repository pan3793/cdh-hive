#!/bin/bash
# The goal of this shell script to bootstrap cloudcat and cluster setup for Hive
# beeline tests, then running the tests on a cluster then destroy that cluster.
# Uses the same parameters as systests because it uses QE/infra_tools/utils.sh
# for common infrastructure needs.
set -ex

ACTIONS="clean setup"
AUTO_PAUSE="None"
JAVA_VERSION="7"
HDFS="basic"
KERBEROS="NONE"
SSL="false"
TLS="false"
JT_HA="false"
LICENSE="enterprise"
PARCELS="true"
CLOUDCAT_PROVISION="true"
DOMAIN="vpc.cloudera.com"
CLOUDCAT_EXPIRATION_DAYS="1"
JAVA7_BUILD=TRUE
if [ -z "$CLUSTER" ]
then
  CLUSTER_SHORTNAME="hive-beeline-test-""${RANDOM}""-{1..4}"
  NEW_CLUSTER=true
else
  CLUSTER_SHORTNAME=$CLUSTER
  NEW_CLUSTER=false
fi

HOSTS_LIST=($(eval echo ${CLUSTER_SHORTNAME}))

# Hiveserver should be on the first node
HIVESERVER2_NODE="${HOSTS_LIST[0]}.${DOMAIN}"
SSH_USER=jenkins

# make the build tools available
. /opt/toolchain/toolchain.sh
echo "Note: utils.sh pulled from master branch"
curl -s -S -O --location http://github.mtv.cloudera.com/QE/infra_tools/raw/master/utils.sh
CDEP_ENV=1
# Convenience functions are imported from this file
# It should be the same as Cluster-Setup job
. utils.sh

if [ "$NEW_CLUSTER" = true ]
then
  # Create the hive safety valves
  OPTIONAL_ARGS="-is=HDFS,YARN,ZOOKEEPER,MAPREDUCE,HIVE,SPARK,SPARK_ON_YARN"
  OPTIONAL_ARGS="${OPTIONAL_ARGS} -jsonurl http://github.mtv.cloudera.com/raw/pvary/notebook/master/hive-beeline/hive-beeline.json"

  # Setup the cluster
  cloudcat_setup

  # Create beeline user 'user' and home directory
  # Is this enough?
  for CLUSTER_NODE in ${HOSTS_LIST[@]}; do
    ssh -o UserKnownHostsFile=/dev/null \
        -o StrictHostKeyChecking=no -q \
        ${SSH_USER}@${CLUSTER_NODE}.${DOMAIN} \
        "sudo useradd -m user"
  done

  ssh -o UserKnownHostsFile=/dev/null \
      -o StrictHostKeyChecking=no -q \
          ${SSH_USER}@${HIVESERVER2_NODE} << __EOF
      sudo -u hdfs hdfs dfs -mkdir /user/user
      sudo -u hdfs hdfs dfs -chown user:user /user/user
__EOF
fi

BEELINE_USER=user
BEELINE_PASSWORD=

DATA_DIR=/run/cloudera-scm-agent
AUX_DIR=/tmp/aux

# Apply patch
cd $WORKSPACE
if [[ -s patch.file ]]
then
  git apply -3 -p0 patch.file
  echo "Patch applied"
else
  echo "No patch file to apply"
fi

# Compiling hive
echo "Compiling hive..."
cd $WORKSPACE
mvn clean install -DskipTests -Phadoop-2
echo "Compiling itests..."
cd $WORKSPACE/itests
mvn clean install -DskipTests -Phadoop-2

# Installing test data
cd $WORKSPACE
tar -cf data.tar data
scp -o UserKnownHostsFile=/dev/null \
    -o StrictHostKeyChecking=no \
    data.tar ${SSH_USER}@${HIVESERVER2_NODE}:/tmp/
ssh -o UserKnownHostsFile=/dev/null \
    -o StrictHostKeyChecking=no -q \
	${SSH_USER}@${HIVESERVER2_NODE} << __EOF
    sudo su -
    mkdir -p ${DATA_DIR}
    cd ${DATA_DIR}
    tar -xf /tmp/data.tar
    mkdir -p ${AUX_DIR}
    chmod a+rwx ${AUX_DIR}
    chown systest:systest -R ${DATA_DIR}/data
    chmod a+rw -R ${DATA_DIR}/data
__EOF

# Upload hive-it-util-*.jar collection to the target machine
scp -o UserKnownHostsFile=/dev/null \
    -o StrictHostKeyChecking=no \
    $WORKSPACE/itests/util/target/hive-it-util-*.jar ${SSH_USER}@${HIVESERVER2_NODE}:${AUX_DIR}

# Load the property file, so we will no which tests to run
cd $WORKSPACE/cloudera/beeline
eval "$(awk -f readproperties.awk testconfiguration.properties)"

# Execute tests
# We execute them in set +e mode (which is not the default in jenkins) to ignore
# the exit code from the maven command.
if [ -n "${beeline_parallel}" ]
then
  echo "Running parallel qtests..."
  cd $WORKSPACE/itests/qtest
  set +e
  mvn clean test -Phadoop-2 -Dtest=TestBeeLineDriver -Dtest.beeline.url="jdbc:hive2://${HIVESERVER2_NODE}:10000" -Dtest.data.dir="${DATA_DIR}/data/files" -Dtest.beeline.user="${BEELINE_USER}" -Dtest.beeline.password="${BEELINE_PASSWORD}" -Dmaven.test.redirectTestOutputToFile=true -Djunit.parallel.timeout=300 -Dqfile="${beeline_parallel}"
  TEST_RESULT=$?
  set -e
  rm -rf target.parallel
  mv target target.parallel
else
  echo "Skipping parallel qtest, since not beeline_parallel is defined..."
fi

if [ -n "${beeline_sequential}" ]
then
  echo "Running sequential qtests..."
  set +e
  mvn clean test -Phadoop-2 -Dtest=TestBeeLineDriver -Dtest.beeline.url="jdbc:hive2://${HIVESERVER2_NODE}:10000" -Dtest.data.dir="${DATA_DIR}/data/files" -Dtest.beeline.user="${BEELINE_USER}" -Dtest.beeline.password="${BEELINE_PASSWORD}" -Dmaven.test.redirectTestOutputToFile=true -Djunit.parallel.timeout=300 -Dqfile="${beeline_sequential}" -Djunit.parallel.threads=1
  TEST_RESULT=$?
  rm -rf target.sequential
  mv target target.sequential
  set -e
else
  echo "Skipping sequential, since not beeline_sequential is defined..."
fi

cd $WORKSPACE/deploy/cdep
# Getting diagnostic bundle
ARGS=("--version=$CM_VERSION")
ARGS+=("--agents=$CDH+parcels@$CLUSTER_SHORTNAME.$DOMAIN")
ARGS+=("--no-locks")
collect_logs_into_workspace

# Finally destroy the cluster
if [[ $KEEP_CLUSTERS_ONLINE == "false" ]]; then
  ${CLOUDCAT_SCRIPT} --hosts=$CLUSTER_SHORTNAME.$DOMAIN \
      --log-dir=$WORKSPACE/cleanup_hosts_logs \
      --username=$CLOUDCAT_USERNAME destroy_group
fi
