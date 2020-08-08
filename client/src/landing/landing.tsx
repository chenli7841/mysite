import React from 'react';
import './landing.css'

function Landing() {
  const bgStyle = {
    backgroundImage: 'url("/images/background1.jpg")',
  };

  return (
    <div className='landing__bg' style={bgStyle}>
      <div className='landing__content'>
        <div className='landing__header'><h1>Li Chen</h1></div>
        <div className='landing__body'>
          <p>
            I am a full-stack developer with more than 6 years experience building enterprise web application.
            I have computer engineering degree from University of Toronto.
            I am working towards the data science degree in University of Illinois Urbana Champaign.
            I am also working on AWS and Azure Certification.
            I love coding and reading social science.
          </p>
          <p>
            My dev skills includes:
          </p>
          <ul className='landing__skills'>
            <li>Backend: Java Spring, .NET, Node.js, Python Tornado</li>
            <li>Frontend: Angular 2+ (NgRx, NgXS), Reactjs (Redux), Google Web Toolkit, jQuery</li>
            <li>Database: SQL Server, MySQL, PostgreSQL, MongoDB, Redis</li>
            <li>AWS: EC2, S3, VPC, SQS, SNS, SES, ElastiCache, Athena, DynamoDB</li>
            <li>CI/CD: Jenkin, GitHub, BitBucket (Bamboo)</li>
            <li>Deployment: Docker, Kubernetes</li>
            <li>Visualization: D3.js, Highchart.js</li>
          </ul>
        </div>
      </div>
    </div>
  );
}

export default Landing;
