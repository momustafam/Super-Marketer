import { useState } from "react";
import { Routes, Route } from "react-router-dom";
import Topbar from './scenes/global/Topbar';
import Sidebar from "./scenes/global/Sidebar";
import Dashboard from './scenes/dashboard';
import TeamMembers from './scenes/teamMembers';
import AddTeamMember from './scenes/addTeamMember';
import Bar from "./scenes/barChart";
import Pie from "./scenes/pieChart";
import Line from "./scenes/lineChart";
import TransLine from "./scenes/transLineChart";
import Geography from "./scenes/geographyChart";
import { ColorModeContext, useMode } from './theme';
import { CssBaseline, ThemeProvider } from '@mui/material';

function App() {
  const [theme, colorMode] = useMode();
  const [isSidebar, setIsSidebar] = useState(true);
  const [isCollapsed, setIsCollapsed] = useState(false);

  // Determine the content class based on sidebar state
  const getContentClass = () => {
    if (!isSidebar) return 'content no-sidebar';
    return `content ${isCollapsed ? 'collapsed' : ''}`;
  };

  return (
    <ColorModeContext.Provider value={colorMode}>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <div className="app">
          {isSidebar && <Sidebar isSidebar={isSidebar} isCollapsed={isCollapsed} setIsCollapsed={setIsCollapsed} />}
          <main className={getContentClass()}>
            <Topbar setIsSidebar={setIsSidebar} />
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/auth/team-members" element={<TeamMembers />} />
              <Route path="/auth/add-team-member" element={<AddTeamMember />} />
              <Route path="/insights/age" element={<Line />} />
              <Route path="/insights/transactions" element={<TransLine />} />
              <Route path="/insights/services" element={<Bar />} />
              <Route path="/insights/gender" element={<Pie />} />
              <Route path="/insights/map" element={<Geography />} />
            </Routes>
          </main>
        </div>
      </ThemeProvider>
    </ColorModeContext.Provider>
  );
}

export default App;
