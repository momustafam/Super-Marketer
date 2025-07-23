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
import { ColorModeContext, useMode } from './theme';
import { CssBaseline, ThemeProvider } from '@mui/material';

function App() {
  const [theme, colorMode] = useMode();
  const [isSidebar, setIsSidebar] = useState(true);

  return (
    <ColorModeContext.Provider value={colorMode}>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <div className="app">
          <Sidebar  isSidebar={isSidebar} />
          <main className="content">
            <Topbar setIsSidebar={setIsSidebar} />
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/auth/team-members" element={<TeamMembers />} />
              <Route path="/auth/add-team-member" element={<AddTeamMember />} />
              <Route path="/insights/age" element={<Line />} />
              <Route path="/insights/services" element={<Bar />} />
              <Route path="/insights/gender" element={<Pie />} />
            </Routes>
          </main>
        </div>
      </ThemeProvider>
    </ColorModeContext.Provider>
  );
}

export default App;
