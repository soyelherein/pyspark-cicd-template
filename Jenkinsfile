pipeline {
  agent {dockerfile true}
  stages {
    stage('build') {
      steps {
        script{
        echo 'pipeline template'
        sh "cd /usr/src/app"
        sh "ls -lrt"
        sh "export PIPENV_VENV_IN_PROJECT=1"
        sh "cd /usr/src/app && pipenv install --dev"
        sh "cd /usr/src/app && pipenv run pytest"
        }
      }
    }
  }
}
