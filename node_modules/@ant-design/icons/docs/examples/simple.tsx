import * as React from 'react';
import styled from 'styled-components';
import { AntDesignOutlined, DashboardOutlined, TwitterOutlined } from '../../src';

const Div = styled.div`
  position: relative;
  margin-bottom: 1rem;
  line-height: 1;
  &:after {
    width: 100%;
    height: 1px;
    content: '';
    position: absolute;
    bottom: 0.125em;
    left: 0;
    background-color: hotpink;
    z-index: -100;
  }
  &:before {
    width: 100%;
    height: 1px;
    content: '';
    position: absolute;
    top: 0.125em;
    left: 0;
    background-color: hotpink;
    z-index: -100;
  }
`;

export default class SimpleDemo extends React.Component<{}> {
  static displaySize = ['64px', '48px', '32px', '24px', '16px', '12px', '8px'];

  static relatedHref =
    'https://blog.prototypr.io/align-' +
    'svg-icons-to-text-and-say-goodbye-to-font-icons-d44b3d7b26b4';

  static renderStatement(fontSize: string, index: number) {
    const style: React.CSSProperties = {
      fontSize,
      color: (fontSize === '48px' && 'lightblue') || 'inherit',
    };
    return (
      <Div key={fontSize + index} style={style}>
        {fontSize}
        Ant Design
        <AntDesignOutlined />
        0123
        <DashboardOutlined />
        你好
        <TwitterOutlined />
      </Div>
    );
  }

  render() {
    return (
      <div>
        <h1>Simple Demo - Icons Alignments</h1>
        {
          SimpleDemo.displaySize.map((fontSize, index) =>
            SimpleDemo.renderStatement(fontSize, index),
          )
        }
        See{' '}
        <a href={SimpleDemo.relatedHref} target="_blank" rel="noopener noreferrer">
          related blog
        </a>{' '}
        for detail.
      </div>
    );
  }
}
