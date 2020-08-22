pipeline {
  agent any
  stages {
    stage('build') {
      steps {
        echo 'pipeline template'
        sh 'pip install -r requirements.txt'
      }
    }
    stage('test') {
      steps {
        sh 'pytest'
      }   
    }
  }
}
