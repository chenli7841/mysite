function registerSwaggerUi(spec_path, base_path, app) {
  const req = require('require-yml');
  const swaggerUi = require('swagger-ui-express');
  const swaggerDocument = req(spec_path);
  swaggerDocument.servers = [ { url: base_path } ];
  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument));
}

module.exports = {
  registerSwaggerUi
}