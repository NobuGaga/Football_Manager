import './app.css';

if (process.env.TARO_ENV === 'h5') {
  require('./h5.css');
}

function App({ children }) {
  return children;
}

export default App;
