import { Typography, Box, IconButton, useTheme } from "@mui/material";
import { useContext } from "react";
import { ColorModeContext, tokens } from "../../theme";
import LightModeOutlinedIcon from "@mui/icons-material/LightModeOutlined";
import DarkModeOutlinedIcon from "@mui/icons-material/DarkModeOutlined";
import NotificationsOutlinedIcon from "@mui/icons-material/NotificationsOutlined";
import SettingsOutlinedIcon from "@mui/icons-material/SettingsOutlined";
import PersonOutlinedIcon from "@mui/icons-material/PersonOutlined";

const Topbar = () => {
    const theme = useTheme();
    const colorMode = useContext(ColorModeContext);

    return (
       <Box display="flex" justifyContent="space-between" alignItems="center" p={2} pb={0}>
            {/* LEFT SIDE */}
            <Typography variant="h3" fontWeight="bold" sx={{ mb: 0 }}>
                Super Marketer
            </Typography>

            {/* RIGHT SIDE ICONS */}
            <Box display="flex" alignItems="center">
                <IconButton onClick={colorMode.toggleColorMode}>
                {theme.palette.mode === "light" ? (
                    <DarkModeOutlinedIcon />
                ) : (
                    <LightModeOutlinedIcon />
                )}
                </IconButton>
                <IconButton>
                <NotificationsOutlinedIcon />
                </IconButton>
                <IconButton>
                <SettingsOutlinedIcon />
                </IconButton>
                <IconButton>
                <PersonOutlinedIcon />
                </IconButton>
            </Box>
        </Box>
    );
};

export default Topbar;