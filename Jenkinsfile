pipeline {
  agent {dockerfile true}
  stages {
    stage('build') {
      steps {
        script{
        echo 'pipeline template'
        sh "pipenv install --dev"
        sh "pipenv run pytest"
        }
      }
    }
  }
}
