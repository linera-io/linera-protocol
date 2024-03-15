import * as React from 'react';
import styled from 'styled-components';
import { blue } from '@ant-design/colors';

import { setTwoToneColor } from '../../src';
import * as AntdIcons from '../../src/icons';

const allIcons: {
  [key: string]: any;
} = AntdIcons;

const iconsList = Object.keys(allIcons)
  .filter(iconName => iconName.includes('TwoTone'));

const Container = styled.div`
  display: flex;
  flex-flow: row wrap;
  width: 80vw;
  margin: auto;
`;

const Card = styled.div`
  height: 90px;
  margin: 12px 0 16px;
  width: 16.6666%;
  text-align: center;
`;

const NameDescription = styled.p`
  display: block;
  text-align: center;
  transform: scale(0.83);
  font-family: 'Lucida Console', Consolas;
  white-space: nowrap;
`;

const Text = styled.span`
  margin: 0 0.5rem;
`;

export default class AllIconDemo extends React.Component {
  state = {
    primaryColor: blue.primary!,
  };

  componentWillMount() {
    setTwoToneColor(this.state.primaryColor);
  }

  onPrimaryColorChange = (e: React.SyntheticEvent<HTMLInputElement>) => {
    setTwoToneColor(e.currentTarget.value);
    this.setState({
      primaryColor: e.currentTarget.value,
    });
  }

  render() {
    return (
      <div style={{ color: '#555' }}>
        <h1 style={{ textAlign: 'center' }}>Two Tone</h1>
        <div style={{ textAlign: 'center' }}>
          <h2>Primary Color</h2>
          <input
            type="color"
            value={this.state.primaryColor}
            onChange={this.onPrimaryColorChange}
          />
          <Text>{this.state.primaryColor}</Text>
        </div>
        <Container>
          {
            iconsList.map(iconName => {
              const Component = allIcons[iconName];
              return (
                <Card key={iconName}>
                  <Component style={{ fontSize: '16px' }} />
                  <NameDescription>{iconName}</NameDescription>
                </Card>
              );
            })
          }
        </Container>
      </div>
    );
  }
}
