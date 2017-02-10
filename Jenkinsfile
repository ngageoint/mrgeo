node ('mrgeo-build'){
  // ---------------------------------------------
  // we want a clean workspace     
  stage ('Clear workspace') {        
  deleteDir()
  }
  
  // ---------------------------------------------
  // checkout code     
  stage ('Checkout'){
  checkout scm
  }
  
  // ---------------------------------------------
  // build using maven     
  stage ('Build'){         
  def mvnHome = "${tool name: 'maven'}"         
  echo "MVN_HOME: ${mvnHome}"
  
  withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: '	c6aeb63d-8a56-4316-b367-3a1fcaae7f3b', passwordVariable: 'PASSWORD', usernameVariable: 'USERNAME']]) {
  
  // setting up confileFileProvider
// configFileProvider([configFile(fileId: '98f8f954-eb23-4a94-b4cf-40df824c0a5c', variable: 'MAVEN_SETTINGS')]) {
   //sh 'mvn -s $MAVEN_SETTINGS clean package'
   
// set up local settings.xml for maven build
  sh '''
  set +x
cat <<-EOF> ${WORKSPACE}/maven-settings.xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <pluginGroups></pluginGroups>
  <proxies></proxies>
  <servers>
    <server>
      <id>mrgeo-maven-plugin</id>
      <!-- access key -->
      <username>AKIAJ4O4S2MEXU5EGGCQ</username>
      <!-- secret key -->
      <password>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</password>
    </server>
    <server>
      <id>mrgeo-maven-release</id>
      <username>AKIAJ4O4S2MEXU5EGGCQ</username>
      <password>90VAcP1jhFU3Lc8Qrnrb9cklMw+j33CckBN9EoiU</password>
    </server>
    <server>
      <id>mrgeo-maven-snapshot</id>
      <username>AKIAJ4O4S2MEXU5EGGCQ</username>
      <password>90VAcP1jhFU3Lc8Qrnrb9cklMw+j33CckBN9EoiU</password>
    </server>
    <server>
      <id>mrgeo-repository</id>
      <username>AKIAJ4O4S2MEXU5EGGCQ</username>
      <password>90VAcP1jhFU3Lc8Qrnrb9cklMw+j33CckBN9EoiU</password>
    </server>
  </servers>
  <mirrors></mirrors>
  <profiles>
    <profile>
      <id>deploy-to-s3</id>
      <properties>
        <repository.url>s3://mrgeo-maven/release</repository.url>
        <snapshot.repository.url>s3://mrgeo-maven/snapshot</snapshot.repository.url>
      </properties>
    </profile>
  </profiles>
  <activeProfiles>
    <activeProfile>deploy-to-s3</activeProfile>
  </activeProfiles>
</settings>
EOF
'''
    
  //env. properties file
  sh '''
  #!/bin/bash
  source /etc/profile
  source ~/.bashrc
  BUILD_VERSION=emr471
  echo "BUILD_VERSION: ${BUILD_VERSION}"
  # Set the existing version of the build
  VERSION=`scripts/mvn-build --quiet help:evaluate -Dexpression=project.version ${BUILD_VERSION} | grep -v \'\\[\' | tail -1`
  echo "VERSION" ${VERSION}
  # Set the build type
  BUILD=`scripts/mvn-build --quiet help:evaluate -Dexpression=final.classifier ${BUILD_VERSION} | grep -v \'\\[\' | tail -1`
  echo "BUILD" ${BUILD}
  # Check for SNAPSHOT, and add the BUILD as part of the version name
  if [[ ${VERSION} == *"-SNAPSHOT" ]]; then
    NEWVERSION=${VERSION%"-SNAPSHOT"}-${BUILD-SNAPSHOT}
  else
    NEWVERSION=${VERSION}-${BUILD}
  fi
  echo "NEWVERSION" ${NEWVERSION}
  mvn_Home="/usr/bin" 
  echo "mvn_Home" ${mvn_Home}
  echo ""

  # set mvn version, build, revert mvn version
  ${mvn_Home}/mvn -Dmodules=all versions:set -DnewVersion=${NEWVERSION}
  ${mvn_Home}/mvn -e -s ${WORKSPACE}/maven-settings.xml -P${BUILD_VERSION} -Pskip-all-tests  -Dmodules=all deploy -U
  ${mvn_Home}/mvn versions:revert
  '''
// }
  }
  }
  
  // ---------------------------------------------
  //archive artifacts     
//  stage ('Archive'){
//  archive '**/distribution-tgz/target/*tar.gz'
//  }
  
  // ---------------------------------------------
  //generate rpm
  stage ('Package MrGeo'){
  sh '''
  #ROOT_WORKSPACE=/jslave/workspace/DigitalGlobe/MrGeo
  echo "Starting packaging..."
  PARENT_TARGET_DIR=${WORKSPACE}/mrgeo-pipeline/distribution/target
  MRGEO_TAR=$(find ${PARENT_TARGET_DIR} -name "mrgeo-*.tar.gz")
  
  mkdir -p ${PARENT_TARGET_DIR}/rpm-creation
  cp ${MRGEO_TAR} ${PARENT_TARGET_DIR}/rpm-creation/
  cd ${PARENT_TARGET_DIR}/rpm-creation
  TARBALL_FILENAME=mrgeo-*.tar.gz
  NEWVERSION=$(echo ${TARBALL_FILENAME})
  TRIMMED_VERSION=${NEWVERSION::-7}
  tar -xvf mrgeo-*.tar.gz
  rm -f mrgeo-*.tar.gz
  #move jar files into jar folder for easier installation
  mkdir jar
  mv *.jar jar/
  echo \'#!/bin/bash\' >> set_mrgeo_env.sh
  echo \'\' >> set_mrgeo_env.sh
  echo \'sudo sh -c "echo export MRGEO_COMMON_HOME=/usr/lib/mrgeo" >> /etc/profile.d/mrgeo.sh\' >> set_mrgeo_env.sh
  echo \'sudo sh -c "echo export MRGEO_CONF_DIR=/etc/mrgeo/conf" >> /etc/profile.d/mrgeo.sh\' >> set_mrgeo_env.sh
  echo \'sudo ln -sf /usr/lib/mrgeo/bin/mrgeo /usr/bin/mrgeo\' >> set_mrgeo_env.sh
  echo \'\' >> set_mrgeo_env.sh
  echo \'\' >> set_mrgeo_env.sh
  echo \'echo "********************** FINISHED MRGEO INSTALLATION *********************"\' >> set_mrgeo_env.sh
  echo \'echo "MrGeo has been installed! Please execute one of these options for MrGeo:"\' >> set_mrgeo_env.sh
  echo \'echo "  1) Restart your shell to activate environment variables OR"\' >> set_mrgeo_env.sh
  echo \'echo "  2) Execute line: source /etc/profile.d/mrgeo.sh"\' >> set_mrgeo_env.sh
  echo \'echo "******************************* ENJOY! *********************************"\' >> set_mrgeo_env.sh
  echo \'\' >> set_mrgeo_env.sh
  chmod +x set_mrgeo_env.sh
  bundle exec fpm -s dir -t rpm -n ${TRIMMED_VERSION}.rpm -p ${TRIMMED_VERSION}.rpm --after-install ./set_mrgeo_env.sh \\
  bin/=/usr/lib/mrgeo/bin/ lib/=/usr/lib/mrgeo/lib/ conf/=/etc/mrgeo/conf/ \\
  color-scales/=/usr/lib/mrgeo/color-scales/ jar/=/usr/lib/mrgeo/
  
  mv ${TRIMMED_VERSION}.rpm ${PARENT_TARGET_DIR}/'''
  }
  
  // ---------------------------------------------
  //generate pymrgeo rpm
  stage ('Package pyMrGeo'){
  sh '''
  #!/bin/bash
  # Set directory var
  #ROOT_WORKSPACE=/jslave/workspace/DigitalGlobe/MrGeo
  MRGEO_DIR=${WORKSPACE}/mrgeo-pipeline
  PYPI_DIR=${MRGEO_DIR}/mrgeo-python/src/main/python
  PARENT_TARGET_DIR=${WORKSPACE}/mrgeo-pipeline/distribution/target
  
  PY_VERSION=0.0.7
  cd ${PYPI_DIR}/
  #clean up existing rpms
  rm -f *.rpm
  # Generate RPM for pymrgeo
  bundle exec fpm -s dir -t rpm -n pymrgeo-${PY_VERSION}.rpm -p pymrgeo-${PY_VERSION}.rpm --prefix /usr/lib/python2.7/dist-packages --directories ./pymrgeo ./pymrgeo
  
  mv ${PYPI_DIR}/pymrgeo-${PY_VERSION}.rpm ${PARENT_TARGET_DIR}/
  '''
  }
}
