using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using TumblThree.Applications.Properties;

namespace TumblThree.Applications.Services
{
    [Export(typeof(IWebRequestFactory))]
    public class WebRequestFactory : IWebRequestFactory
    {
        private readonly AppSettings settings;

        [ImportingConstructor]
        public WebRequestFactory(AppSettings settings)
        {
            this.settings = settings;
        }

        private HttpWebRequest CreateStubReqeust(string url, string referer = "", Dictionary<string, string> headers = null)
        {
            var request = (HttpWebRequest)WebRequest.Create(url);
            request.ProtocolVersion = HttpVersion.Version11;
            request.UserAgent = settings.UserAgent;
            request.AllowAutoRedirect = true;
            request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

            //request.KeepAlive = true;
            //request.Pipelined = true;

            // Timeouts don't work with GetResponseAsync() as it internally uses BeginGetResponse.
            // See docs: https://msdn.microsoft.com/en-us/library/system.net.httpwebrequest.timeout(v=vs.110).aspx
            // Quote: The Timeout property has no effect on asynchronous requests made with the BeginGetResponse or BeginGetRequestStream method.
            // TODO: Use HttpClient instead?

            request.ReadWriteTimeout = settings.TimeOut * 1000;
            request.Timeout = settings.TimeOut * 1000;
            request.CookieContainer = new CookieContainer();
            request.CookieContainer.PerDomainCapacity = 100;
            ServicePointManager.DefaultConnectionLimit = 400;
            request = SetWebRequestProxy(request, settings);
            request.Referer = referer;
            if (headers == null)
            {
                return request;
            }
            foreach (KeyValuePair<string, string> header in headers)
            {
                request.Headers[header.Key] = header.Value;
            }
            return request;
        }

        public HttpWebRequest CreateGetReqeust(string url, string referer = "", Dictionary<string, string> headers = null)
        {
            HttpWebRequest request = CreateStubReqeust(url, referer, headers);
            request.Method = "GET";
            return request;
        }

        public HttpWebRequest CreateGetXhrReqeust(string url, string referer = "", Dictionary<string, string> headers = null)
        {
            HttpWebRequest request = CreateStubReqeust(url, referer, headers);
            request.Method = "GET";
            request.ContentType = "application/json";
            request.Headers["X-Requested-With"] = "XMLHttpRequest";
            return request;
        }

        public HttpWebRequest CreatePostReqeust(string url, string referer = "", Dictionary<string, string> headers = null)
        {
            HttpWebRequest request = CreateStubReqeust(url, referer, headers);
            request.Method = "POST";
            request.ContentType = "application/x-www-form-urlencoded";
            return request;
        }

        public HttpWebRequest CreatePostXhrReqeust(string url, string referer = "", Dictionary<string, string> headers = null)
        {
            HttpWebRequest request = CreatePostReqeust(url, referer, headers);
            request.Accept = "application/json, text/javascript, */*; q=0.01";
            request.Headers["X-Requested-With"] = "XMLHttpRequest";
            return request;
        }

        public async Task<bool> RemotePageIsValid(string url)
        {
            HttpWebRequest request = CreateStubReqeust(url);
            request.Method = "HEAD";
            request.AllowAutoRedirect = false;
            var response = await request.GetResponseAsync() as HttpWebResponse;
            response.Close();
            return (response.StatusCode == HttpStatusCode.OK);
        }

        public async Task<string> ReadReqestToEnd(HttpWebRequest request)
        {
            using (var response = await request.GetResponseAsync() as HttpWebResponse)
            {
                using (var stream = GetStreamForApiRequest(response.GetResponseStream()))
                {
                    using (var buffer = new BufferedStream(stream))
                    {
                        using (var reader = new StreamReader(buffer))
                        {
                            return reader.ReadToEnd();
                        }
                    }
                }
            }
        }

        public Stream GetStreamForApiRequest(Stream stream)
        {
            if (!settings.LimitScanBandwidth || settings.Bandwidth == 0)
                return stream;
            return new ThrottledStream(stream, (settings.Bandwidth / settings.ConcurrentConnections) * 1024);

        }

        public string UrlEncode(IDictionary<string, string> parameters)
        {
            var sb = new StringBuilder();
            foreach (KeyValuePair<string, string> val in parameters)
            {
                sb.AppendFormat("{0}={1}&", val.Key, HttpUtility.UrlEncode(val.Value));
            }
            sb.Remove(sb.Length - 1, 1); // remove last '&'
            return sb.ToString();
        }

        private static HttpWebRequest SetWebRequestProxy(HttpWebRequest request, AppSettings settings)
        {
            if (!string.IsNullOrEmpty(settings.ProxyHost) && !string.IsNullOrEmpty(settings.ProxyPort))
            {
                request.Proxy = new WebProxy(settings.ProxyHost, int.Parse(settings.ProxyPort));
            }
            if (!string.IsNullOrEmpty(settings.ProxyUsername) && !string.IsNullOrEmpty(settings.ProxyPassword))
            {
                request.Proxy.Credentials = new NetworkCredential(settings.ProxyUsername, settings.ProxyPassword);
            }
            return request;
        }
    }
}