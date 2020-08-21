pipeline {
  agent { docker { image 'python:3.7.2' } }
  stages {
    stage('build') {
      steps {
        echo 'pipeline template'
        sh 'pip install -r requirements.txt'
        pytest
      }
    }
    stage('test') {
      steps {
        sh 'pytest'
      }   
    }
  }
}
