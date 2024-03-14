import * as React from 'react';
import {
  AntDesignOutlined, DashboardOutlined, SmileOutlined,
  SyncOutlined, TwitterOutlined, LoadingOutlined,
  HomeOutlined, SettingFilled, createFromIconfontCN,
} from '../../src';
import IconContext from '../../src/components/Context';

const IconFont = createFromIconfontCN({
  scriptUrl: '//at.alicdn.com/t/font_8d5l8fzk5b87iudi.js',
});

const Basic = () => (
  <IconContext.Provider value={{rootClassName: 'hashCls'}}>
    <HomeOutlined />
    <SettingFilled />
    <IconFont type="icon-tuichu" />
  </IconContext.Provider>
);

export default Basic;
