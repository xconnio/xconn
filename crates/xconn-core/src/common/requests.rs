// Session-bound request builders share configuration and execution semantics.
macro_rules! session_request_builders {
    (($($asyncness:tt)*) ($($wait:tt)*)) => {
        /// A configured call operation. Nothing is sent until `execute`.
        #[must_use = "Requests do nothing until execute() is called"]
        pub struct CallBuilder<'a> {
            pub(super) session: &'a super::session::Session,
            pub(super) request: crate::CallRequest,
        }

        impl CallBuilder<'_> {
            pub fn arg(mut self, value: impl Into<crate::Value>) -> Self {
                self.request = self.request.arg(value);
                self
            }

            pub fn args(mut self, values: Vec<crate::Value>) -> Self {
                self.request = self.request.args(values);
                self
            }

            pub fn kwargs(mut self, values: std::collections::HashMap<String, crate::Value>) -> Self {
                self.request = self.request.kwargs(values);
                self
            }

            pub fn kwarg(mut self, key: &str, value: impl Into<crate::Value>) -> Self {
                self.request = self.request.kwarg(key, value);
                self
            }

            pub fn option(mut self, key: &str, value: impl Into<crate::Value>) -> Self {
                self.request = self.request.option(key, value);
                self
            }

            pub fn options(mut self, values: std::collections::HashMap<String, crate::Value>) -> Self {
                self.request = self.request.options(values);
                self
            }

            pub $($asyncness)* fn execute(self) -> Result<crate::RpcResult, crate::Error> {
                self.session.execute_call(self.request)$($wait)*
            }
        }

        /// A configured publish operation. Nothing is sent until `execute`.
        #[must_use = "Requests do nothing until execute() is called"]
        pub struct PublishBuilder<'a> {
            pub(super) session: &'a super::session::Session,
            pub(super) request: crate::PublishRequest,
        }

        impl PublishBuilder<'_> {
            pub fn arg(mut self, value: impl Into<crate::Value>) -> Self {
                self.request = self.request.arg(value);
                self
            }

            pub fn args(mut self, values: Vec<crate::Value>) -> Self {
                self.request = self.request.args(values);
                self
            }

            pub fn kwargs(mut self, values: std::collections::HashMap<String, crate::Value>) -> Self {
                self.request = self.request.kwargs(values);
                self
            }

            pub fn kwarg(mut self, key: &str, value: impl Into<crate::Value>) -> Self {
                self.request = self.request.kwarg(key, value);
                self
            }

            pub fn option(mut self, key: &str, value: impl Into<crate::Value>) -> Self {
                self.request = self.request.option(key, value);
                self
            }

            pub fn options(mut self, values: std::collections::HashMap<String, crate::Value>) -> Self {
                self.request = self.request.options(values);
                self
            }

            pub $($asyncness)* fn execute(self) -> Result<Option<crate::PublishResponse>, crate::Error> {
                self.session.execute_publish(self.request)$($wait)*
            }
        }

        /// A configured register operation. Nothing is sent until `execute`.
        #[must_use = "Requests do nothing until execute() is called"]
        pub struct RegisterBuilder<'a> {
            pub(super) session: &'a super::session::Session,
            pub(super) request: super::types::RegisterRequest,
        }

        impl RegisterBuilder<'_> {
            pub fn option(mut self, key: &str, value: impl Into<crate::Value>) -> Self {
                self.request = self.request.with_option(key, value);
                self
            }

            pub fn options(mut self, values: std::collections::HashMap<String, crate::Value>) -> Self {
                self.request = self.request.with_options(values);
                self
            }

            pub $($asyncness)* fn execute(self) -> Result<crate::RegisterResponse, crate::Error> {
                self.session.execute_register(self.request)$($wait)*
            }
        }

        /// A configured subscribe operation. Nothing is sent until `execute`.
        #[must_use = "Requests do nothing until execute() is called"]
        pub struct SubscribeBuilder<'a> {
            pub(super) session: &'a super::session::Session,
            pub(super) request: super::types::SubscribeRequest,
        }

        impl SubscribeBuilder<'_> {
            pub fn option(mut self, key: &str, value: impl Into<crate::Value>) -> Self {
                self.request = self.request.with_option(key, value);
                self
            }

            pub fn options(mut self, values: std::collections::HashMap<String, crate::Value>) -> Self {
                self.request = self.request.with_options(values);
                self
            }

            pub $($asyncness)* fn execute(self) -> Result<crate::SubscribeResponse, crate::Error> {
                self.session.execute_subscribe(self.request)$($wait)*
            }
        }
    };
}

pub(crate) use session_request_builders;
