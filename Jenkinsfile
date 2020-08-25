pipeline {
  agent {dockerfile {
  args "-u jenkins"}
  }
  stages {
    stage("prepare") {
      steps {
        script{
        echo 'pipeline template'
        sh "pipenv install --dev"
        sh "pipenv run pytest"
        }
      }
    }
    stage('test'){
      steps{
        sh "pipenv run pytest"
      }
    }
  }
}
