pipeline {
  agent {dockerfile true}
  stages {
    stage('build') {
      steps {
        script{
        echo 'pipeline template'
        sh 'echo $JAVA_HOME'
        sh 'java -version'
        sh 'echo $PATH'
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
