import {
  Box,
  // Button,
  // IconButton,
  Typography,
  useTheme,
} from "@mui/material";
import { tokens } from "../../theme";
import RepeatIcon  from "@mui/icons-material/Repeat";
import AttachMoneyIcon  from "@mui/icons-material/AttachMoney";
import PersonAddIcon from "@mui/icons-material/PersonAdd";
import PointOfSaleIcon from "@mui/icons-material/PointOfSale";
import TrendingUpIcon from "@mui/icons-material/TrendingUp";
import TrendingDownIcon from "@mui/icons-material/TrendingDown";
import Header from "../../components/Header";
import LineChart from "../../components/LineChart";
import GeographyChart from "../../components/GeographyChart";
import BarChart from "../../components/BarChart";
import PieChart from "../../components/PieChart";
import StatBox from "../../components/StatBox";
// import ProgressCircle from "../../components/ProgressCircle";

const Dashboard = () => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);

  return (
    <Box m="20px">
      {/* HEADER */}
      <Box display="flex" justifyContent="space-between" alignItems="center">
        <Header title="Dashboard Overview" subtitle="Insights and Metrics at a Glance" />

        {/* <Box>
          <Button
            sx={{
              backgroundColor: colors.blueAccent[700],
              color: colors.gray[100],
              fontSize: "14px",
              fontWeight: "bold",
              padding: "10px 20px",
            }}
          >
            <DownloadOutlinedIcon sx={{ mr: "10px" }} />
            Download Reports
          </Button>
        </Box> */}
      </Box>

      {/* GRID & CHARTS */}
      <Box
        display="grid"
        gridTemplateColumns="repeat(12, 1fr)"
        gridAutoRows="140px"
        gap="20px"
        sx={{
          "& > div": {
            gridColumn: "span 12",
          },
          "& > div:nth-of-type(1), & > div:nth-of-type(2), & > div:nth-of-type(3), & > div:nth-of-type(4)":
            {
              gridColumn: {
                xs: "span 12",
                sm: "span 6",
                md: "span 3",
              },
            },
          "& > div:nth-of-type(5), & > div:nth-of-type(6)": {
            gridColumn: {
              xs: "span 12",
              md: "span 6",
            },
            gridRow: {
              xs: "span 2",
              md: "span 2",
            },
          },
          "& > div:nth-of-type(7), & > div:nth-of-type(8), & > div:nth-of-type(9)": {
            gridColumn: {
              xs: "span 12",
              md: "span 4",
            },
            gridRow: {
              xs: "span 2",
              md: "span 2",
            },
          },
        }}
      >
        {/* ROW 1 */}
        <Box
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
        >
          <StatBox
            apiEndpoint="/api/stats/new-users/test"
            enableApi={true}
            icon={
              <PersonAddIcon sx={{ color: colors.greenAccent[600], fontSize: "26px" }} />
            }
            trendline={
              <TrendingUpIcon sx={{ color: colors.greenAccent[600], fontSize: "26px" }} />
            }
          />
        </Box>

        <Box
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
        >
          <StatBox
            apiEndpoint="/api/stats/returning-users/test"
            enableApi={true}
            icon={
              <RepeatIcon sx={{ color: colors.greenAccent[600], fontSize: "26px" }} />
            }
            trendline={
              <TrendingUpIcon sx={{ color: colors.greenAccent[600], fontSize: "26px" }} />
            }
          />
        </Box>

        <Box
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
        >
          <StatBox
            apiEndpoint="/api/stats/transactions/test"
            enableApi={true}
            icon={
              <PointOfSaleIcon sx={{ color: colors.greenAccent[600], fontSize: "26px" }} />
            }
            trendline={
              <TrendingUpIcon sx={{ color: colors.greenAccent[600], fontSize: "26px" }} />
            }
          />
        </Box>

        <Box
          backgroundColor={colors.primary[400]}
          display="flex"
          alignItems="center"
          justifyContent="center"
        >
          <StatBox
            apiEndpoint="/api/stats/clv/test"
            enableApi={true}
            icon={
              <AttachMoneyIcon sx={{ color: colors.greenAccent[600], fontSize: "26px" }} />
            }
            trendline={
              <TrendingDownIcon sx={{ color: colors.redAccent[500], fontSize: "26px" }} />
            }
          />
        </Box>
        {/* ROW 2 */}
        <Box backgroundColor={colors.primary[400]}>
          <Box
            mt="25px"
            p="0 30px"
            display="flex "
            justifyContent="space-between"
            alignItems="center"
          >
            <Box>
              <Typography
                variant="h4"
                fontWeight="600"
                color={colors.gray[100]}
              >
                Age Distribution
              </Typography>
            </Box>
            {/* <Box>
              <IconButton>
                <DownloadOutlinedIcon
                  sx={{ fontSize: "26px", color: colors.greenAccent[500] }}
                />
              </IconButton>
            </Box> */}
          </Box>
          <Box height="250px" m="-20px 0 0 0">
            <LineChart isDashboard={true} chartType="age-distribution-line" />
          </Box>
        </Box>
        <Box backgroundColor={colors.primary[400]}>
          <Box
            mt="25px"
            p="0 30px"
            display="flex "
            justifyContent="space-between"
            alignItems="center"
          >
            <Box>
              <Typography
                variant="h4"
                fontWeight="600"
                color={colors.gray[100]}
              >
                Transactions by Hour
              </Typography>
            </Box>
            {/* <Box>
              <IconButton>
                <DownloadOutlinedIcon
                  sx={{ fontSize: "26px", color: colors.greenAccent[500] }}
                />
              </IconButton>
            </Box> */}
          </Box>
          <Box height="250px" m="-20px 0 0 0">
            <LineChart isDashboard={true} chartType="transactions-by-hour" />
          </Box>
        </Box>
        {/* ROW 3 */}
        <Box backgroundColor={colors.primary[400]}>
          <Typography
            variant="h5"
            fontWeight="600"
            sx={{ padding: "30px 30px 0 30px" }}
          >
            Services Usage
          </Typography>
          <Box height="250px" mt="-20px">
            <BarChart isDashboard={true} />
          </Box>
        </Box>
        <Box backgroundColor={colors.primary[400]} padding="30px">
          <Typography
            variant="h5"
            fontWeight="600"
            sx={{ marginBottom: "15px" }}
          >
            Users Gender
          </Typography>
          <Box height="200px">
            <PieChart isDashboard={true} />
          </Box>
        </Box>
        <Box backgroundColor={colors.primary[400]} padding="30px">
          <Typography
            variant="h5"
            fontWeight="600"
            sx={{ marginBottom: "15px" }}
          >
            Geography Based Traffic
          </Typography>
          <Box height="200px">
            <GeographyChart isDashboard={true} />
          </Box>
        </Box>
      </Box>
    </Box>
  );
};

export default Dashboard;
