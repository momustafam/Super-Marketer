import { useState } from "react";
import { Sidebar as ProSidebar, Menu, MenuItem } from "react-pro-sidebar";
import { Box, IconButton, Typography, useTheme } from "@mui/material";
import { Link } from "react-router-dom";
import { tokens } from "../../theme";
import HomeOutlinedIcon from "@mui/icons-material/HomeOutlined";
import PeopleOutlinedIcon from "@mui/icons-material/PeopleOutlined";
import PersonOutlinedIcon from "@mui/icons-material/PersonOutlined";
import RateReviewOutlinedIcon from "@mui/icons-material/RateReviewOutlined";
import BarChartOutlinedIcon from "@mui/icons-material/BarChartOutlined";
import DonutLargeOutlinedIcon from "@mui/icons-material/DonutLargeOutlined";
import PublicOutlinedIcon from "@mui/icons-material/PublicOutlined";
import MenuOutlinedIcon from "@mui/icons-material/MenuOutlined";
import AreaChartIcon from "@mui/icons-material/AreaChart";
import TimelineOutlinedIcon from "@mui/icons-material/TimelineOutlined";
import SmartToyOutlinedIcon from "@mui/icons-material/SmartToyOutlined";

const Item = ({ title, to, icon, selected, setSelected }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);

  return (
    <MenuItem
      active={selected === title}
      icon={icon}
      onClick={() => setSelected(title)}
      style={{
        color: colors.gray[100],
      }}
      component={<Link to={to} />}
    >
      <Typography>{title}</Typography>
    </MenuItem>
  );
};

const Sidebar = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);
  const [isCollapsed, setIsCollapsed] = useState(false);
  const [selected, setSelected] = useState("Dashboard");

  return (
    <Box
      sx={{
        height: "100%",
        zIndex: 1000,
        "& .ps-sidebar-root": {
          border: "none",
        },
        "& .ps-sidebar-container": {
          background: `${colors.primary[400]} !important`,
        },
        "& .ps-menu-button": {
          padding: "5px 35px 5px 20px !important",
        },
        "& .ps-menu-button:hover": {
          color: "#868dfb !important",
        },
        "& .ps-menu-button.ps-active": {
          color: "#6870fa !important",
        },
      }}
    >
      <ProSidebar collapsed={isCollapsed}>
        <Menu>
          {/* LOGO AND MENU ICON */}
          <MenuItem
            onClick={() => setIsCollapsed(!isCollapsed)}
            icon={isCollapsed ? <MenuOutlinedIcon /> : undefined}
            style={{
              margin: "10px 0 20px 0",
              color: colors.gray[100],
            }}
          >
            {!isCollapsed && (
                <Box
                sx={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    ml: "15px"
                }}
                >
                    <Typography variant="h3" color={colors.gray[100]}>
                        Super Marketer
                    </Typography>
                    <IconButton onClick={() => setIsCollapsed(!isCollapsed)}>
                        <MenuOutlinedIcon />
                    </IconButton>
                </Box>
            )}
          </MenuItem>

          {/* USER PROFILE */}
          {!isCollapsed && (
            <Box sx={{mb: "25px"}}>
                <Box 
                sx={{
                    display: "flex",
                    justifyContent: "center",
                    alignItems:"center"
                }}>
                <img
                src="../../assets/user.png"
                alt="profile-user"
                style={{
                    width: "100px",
                    height: "100px",
                    cursor: "pointer",
                    borderRadius: "50%",
                }}
                />
              </Box>
              <Box sx={{textAlign: "center"}}>
                <Typography
                  variant="h4"
                  color={colors.gray[100]}
                  fontWeight="bold"
                  sx={{ m: "10px 0 0 0" }}
                >
                  Mahmoud Afify
                </Typography>
                <Typography variant="h5" color={colors.greenAccent[500]}>
                  Chief Technology Officer
                </Typography>
              </Box>
            </Box>
          )}

          {/* MENU ITEMS */}
          <Box paddingLeft={isCollapsed ? undefined : "10%"}>
            <Item
              title="Dashboard"
              to="/"
              icon={<HomeOutlinedIcon />}
              selected={selected}
              setSelected={setSelected}
            />
            <Typography
              variant="h6"
              color={colors.gray[300]}
              sx={{ m: "15px 0 5px 20px" }}
            >
              Authentication
            </Typography>
            <Item
              title="Team Members"
              to="/auth/team-members"
              icon={<PeopleOutlinedIcon />}
              selected={selected}
              setSelected={setSelected}
            />
            <Item
                title="Add Team Member"
                to="/auth/add-team-member"
                icon={<PersonOutlinedIcon />}
                selected={selected}
                setSelected={setSelected}
            />
            <Typography
              variant="h6"
              color={colors.gray[300]}
              sx={{ m: "15px 0 5px 20px" }}
            >
              Advanced Tools
            </Typography>
            <Item
                title="Reviews Scrapper"
                to="/tools/reviews"
                icon={<RateReviewOutlinedIcon />}
                selected={selected}
                setSelected={setSelected}
            />
            <Item
                title="AI Assistant"
                to="/tools/ai_assistant"
                icon={<SmartToyOutlinedIcon />}
                selected={selected}
                setSelected={setSelected}
            />
            <Typography
                variant="h6"
                color={colors.gray[300]}
                sx={{ m: "15px 0 5px 20px" }}
            >
                Insights
            </Typography>
            <Item
                title="Age Distribution"
                to="/insights/age"
                icon={<AreaChartIcon />}
                selected={selected}
                setSelected={setSelected}
            />
            <Item
                title="Transactions by Day"
                to="/insights/transactions"
                icon={<TimelineOutlinedIcon />}
                selected={selected}
                setSelected={setSelected}
            />
            <Item
                title="Services Usage"
                to="/insights/services"
                icon={<BarChartOutlinedIcon />}
                selected={selected}
                setSelected={setSelected}
            />
            <Item
                title="Density Map"
                to="/insights/map"
                icon={<PublicOutlinedIcon />}
                selected={selected}
                setSelected={setSelected}
            />
            <Item
                title="Users Gender"
                to="/insights/gender"
                icon={<DonutLargeOutlinedIcon />}
                selected={selected}
                setSelected={setSelected}
            />
        </Box>
        </Menu>
      </ProSidebar>
    </Box>
  );
};

export default Sidebar;