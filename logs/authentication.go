package logs

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const allowedContentType = "application/logplex-1"
const logplexDrainTokenPrefix = "d."

// Authentication gin middleware to validate that requests to /logs server are legitimate
func Authentication() gin.HandlerFunc {
	return func(c *gin.Context) {
		// skip GET root requests since we redirect to postgresmonitor.com
		if c.Request.Method != "GET" && c.Request.URL.Path != "/" {
			// validate content type
			contentType := c.Request.Header.Get("Content-Type")
			if contentType != allowedContentType {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}

			// validate logplex drain token
			drainToken := c.Request.Header.Get("Logplex-Drain-Token")
			if len(drainToken) == 0 || !strings.HasPrefix(drainToken, logplexDrainTokenPrefix) {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
		}

		c.Next()
	}
}
