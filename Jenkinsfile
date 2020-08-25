pipeline {
  agent {dockerfile {
  args '-u root --privileged'}
  }
  stages {
    stage('build') {
      steps {
        script{
        echo 'pipeline template'
        sh 'whoami'
        sh 'sudo su'
        sh 'echo $JAVA_HOME'
        sh 'java -version'
        sh 'echo $PATH'
        sh 'whoami'
        sh 'echo $PYSPARK_SUBMIT_ARGS'
        sh 'echo $PIPENV_VENV_IN_PROJECT'
        sh 'echo $PIPENV_CACHE_DIR'
        sh "cd /usr/src/app"
        sh "ls -lrt"
        sh "pipenv install --dev"
        sh "which python3"
        sh "pipenv run pytest"
        }
      }
    }
  }
}
