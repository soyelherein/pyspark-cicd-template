pipeline {
  agent {dockerfile {
  args "-u jenkins"}
  }
  stages {
    stage("prepare") {
      steps {
        script{
        echo "pipeline template"
        sh "ls -lart"
        sh "pipenv install --dev"
        }
      }
    }
    stage("test"){
      steps{
        sh "pipenv run pytest"
      }
    }
    stage("prepare artifacts"){
      steps{
        sh "make build"
      }
    }
  }
}
