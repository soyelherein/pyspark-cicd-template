pipeline {
  agent any
  stages {
    stage('build') {
      steps {
        echo 'pipeline template'
        sh 'python -m pytest'
      }
    }
    stage('test') {
      steps {
        sh 'pytest'
      }   
    }
  }
}
