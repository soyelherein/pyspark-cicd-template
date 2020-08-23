pipeline {
  agent {docker { image 'python:3.7.2' }}
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
