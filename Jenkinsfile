pipeline {
  agent {dockerfile true}
  stages {
    stage('build') {
      steps {
        script{
        echo 'pipeline template'
        sh "cd /usr/src/app"
        sh "ls -lrt"
        sh "pipenv install --dev"
        sh "pipenv run pytest"
        }
      }
    }
  }
}
