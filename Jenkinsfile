pipeline {
  agent {dockerfile true}
  stages {
    stage('build') {
      steps {
        script{
        echo 'pipeline template'
        sh "cd /usr/src/app"
        sh "cd /usr/src/app && pipenv install --dev"
        sh "cd /usr/src/app && pipenv run pytest"
        }
      }
    }
  }
}
